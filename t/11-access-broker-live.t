use strict;
use warnings;
use Test::More;
use Test::Exception;

BEGIN {
  plan skip_all => 'Set DBIOTEST_PG_DSN to run integration tests'
    unless $ENV{DBIOTEST_PG_DSN};
  eval { require EV::Pg } or plan skip_all => 'EV::Pg not installed';
}

use EV;
use DBIO::AccessBroker;
use DBIO::PostgreSQL::Async;
use DBIO::PostgreSQL::Async::Storage;

# --- Parse DSN into libpq conninfo hashref ---

my $dsn  = $ENV{DBIOTEST_PG_DSN};
my $user = $ENV{DBIOTEST_PG_USER} || '';
my $pass = $ENV{DBIOTEST_PG_PASS} || '';

my %base_conninfo;
if ($dsn =~ /^dbi:Pg:(.+)/i) {
  my $params = $1;
  for my $pair (split /;/, $params) {
    my ($k, $v) = split /=/, $pair, 2;
    $base_conninfo{$k} = $v if defined $k && defined $v;
  }
  $base_conninfo{user}     = $user if $user;
  $base_conninfo{password} = $pass if $pass;
} else {
  # Raw libpq conninfo string — wrap as-is
  %base_conninfo = (conninfo_string => $dsn);
  $base_conninfo{user}     = $user if $user;
  $base_conninfo{password} = $pass if $pass;
}

# --- Test broker that wraps real conninfo ---

{
  package LiveBroker;
  use base 'DBIO::AccessBroker';

  sub new {
    my ($class, %conninfo) = @_;
    bless {
      conninfo => \%conninfo,
      calls    => 0,
      refresh  => 0,
    }, $class;
  }

  sub refresh       { $_[0]->{refresh}++ }
  sub needs_refresh { $_[0]->{refresh} > 0 }
  sub calls         { $_[0]->{calls}   }

  sub connect_info_for_storage {
    my ($self, $storage, $mode) = @_;
    $self->{calls}++;
    $self->{refresh} = 0;
    return [ { %{ $self->{conninfo} } }, {} ];
  }
}

my $broker = LiveBroker->new(%base_conninfo);

# ===== 1. Storage-level broker attachment =====

my $storage = DBIO::PostgreSQL::Async::Storage->new(undef);
$storage->connect_info([$broker]);

is $storage->access_broker, $broker,
  'storage->access_broker returns the broker after connect_info([$broker])';
is $storage->access_broker_mode, 'write',
  'broker mode defaults to write';

my $provider = $storage->_conninfo_provider;
ok ref $provider eq 'CODE',
  'storage exposes a conninfo provider coderef when broker is attached';

my $calls_before = $broker->calls;
$provider->();
is $broker->calls, $calls_before + 1,
  'conninfo provider calls connect_info_for_storage on each invocation';

# ===== 2. Schema->connect($broker) path =====

{
  package TestSchema;
  use base 'DBIO::Schema';
  __PACKAGE__->load_components('PostgreSQL::Async');
}

my $schema = TestSchema->connect($broker);
isa_ok $schema->storage, 'DBIO::PostgreSQL::Async::Storage',
  'Schema->connect($broker) creates async storage';
is $schema->storage->access_broker, $broker,
  'schema storage has broker attached';
is $schema->storage->access_broker_mode, 'write',
  'schema storage broker mode defaults to write';

# ===== 3. Live connectivity via broker =====

my $live_storage = DBIO::PostgreSQL::Async::Storage->new(undef);
$live_storage->connect_info([$broker]);

my $calls_at_pool = $broker->calls;

# Pool is constructed lazily — pool->acquire triggers _conninfo_provider
my $connected = 0;
my $connected_err;

{
  my $f_connected = Future->new;

  # Use the underlying pool to verify a real connection is established
  my $pg;
  eval {
    $pg = EV::Pg->new(
      conninfo   => $live_storage->_conninfo_string,
      on_connect => sub { $f_connected->done(1) unless $f_connected->is_ready },
      on_error   => sub {
        $connected_err = $_[0];
        $f_connected->done(0) unless $f_connected->is_ready;
      },
    );
  };
  if ($@) {
    plan skip_all => "EV::Pg->new failed: $@";
  }

  EV::run until $f_connected->is_ready;
  $connected = $f_connected->get;

  if (!$connected) {
    plan skip_all => "Could not connect to PostgreSQL via broker conninfo: $connected_err";
  }

  # Quick sanity query
  my $done = 0;
  my ($rows, $err);
  $pg->query_params('SELECT $1::text AS source', ['access_broker'], sub {
    ($rows, $err) = @_;
    $done = 1;
  });
  EV::run until $done;

  ok !$err, 'no error on broker-conninfo query';
  is $rows->[0][0], 'access_broker',
    'live query works with conninfo from broker';

  $pg->finish;
}

ok $connected, 'connected to PostgreSQL using broker-supplied conninfo';

# ===== 4. Broker refresh causes new conninfo fetch =====

my $calls_before_refresh = $broker->calls;
$broker->refresh;

is $broker->needs_refresh, 1, 'broker needs_refresh after ->refresh';

my $info_after_refresh = $live_storage->_current_async_connect_info('write');
ok defined $info_after_refresh, '_current_async_connect_info returns data after refresh';
is $broker->calls, $calls_before_refresh + 1,
  '_current_async_connect_info fetched fresh info from broker after refresh';
ok !$broker->needs_refresh,
  'needs_refresh cleared after connect_info_for_storage call';

# ===== 5. conninfo_provider continues to use broker after refresh =====

$broker->refresh;
my $calls_before_provider = $broker->calls;
$provider = $live_storage->_conninfo_provider;
$provider->();
is $broker->calls, $calls_before_provider + 1,
  'conninfo provider still calls broker after second refresh';

# ===== 6. LISTEN connection uses broker conninfo =====

SKIP: {
  skip 'LISTEN test requires a full EV::Pg connection', 3 unless $connected;

  # Start a dedicated LISTEN connection via the storage
  my $notify_received = 0;
  my $listen_pg;

  my $listen_connected = 0;
  eval {
    require EV::Pg;
    $listen_pg = EV::Pg->new(
      conninfo   => $live_storage->_conninfo_string,
      on_connect => sub { $listen_connected = 1 },
      on_error   => sub { warn "listen test error: $_[0]\n" },
      on_notify  => sub {
        my ($ch, $payload) = @_;
        $notify_received++ if $ch eq '_dbio_test_notify';
      },
    );
  };
  skip "Could not create LISTEN connection: $@", 3 if $@;

  # Use RUN_ONCE throughout — keep_alive watchers never let EV::run() return
  EV::run(EV::RUN_ONCE) until $listen_connected;

  my $listening = 0;
  $listen_pg->query('LISTEN _dbio_test_notify', sub { $listening = 1 });
  EV::run(EV::RUN_ONCE) until $listening;

  ok $listening, 'LISTEN connection established via broker conninfo';

  # Send a NOTIFY from a separate connection
  my $notified = 0;
  my $notify_pg;
  $notify_pg = EV::Pg->new(
    conninfo   => $live_storage->_conninfo_string,
    on_connect => sub {
      $notify_pg->query("NOTIFY _dbio_test_notify, 'hello'", sub {
        $notified = 1;
      });
    },
    on_error => sub {},
  );
  EV::run(EV::RUN_ONCE) until $notified;

  ok $notified, 'NOTIFY sent via broker conninfo connection';

  # Timer ensures we don't spin forever if the notification is lost
  my $waited = 0;
  my $timer = EV::timer(1.0, 0, sub { $waited = 1 });
  EV::run(EV::RUN_ONCE) until $waited || $notify_received;

  ok $notify_received, 'NOTIFY delivered to LISTEN connection via broker conninfo';

  $listen_pg->finish;
  $notify_pg->finish;
}

done_testing;
