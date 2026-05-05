use strict;
use warnings;
use Test::More;

# Mock EV::Pg so we don't need a real PostgreSQL
{
  package MockEvPg;
  my @queries;
  sub new {
    my ($class, %args) = @_;
    return bless {
      on_connect => $args{on_connect},
      on_notify  => $args{on_notify},
    }, $class;
  }
  sub query {
    my ($self, $sql, $cb) = @_;
    push @queries, $sql;
    # simulate async connect firing immediately on first query
    $self->{on_connect}->() if !$self->{_connected}++;
  }
  sub queries { \@queries }
  sub fire_notify {
    my ($self, $ch, $payload) = @_;
    $self->{on_notify}->($ch, $payload);
  }
}

# Patch _create_pg_connection in ListenManager to use MockEvPg
use DBIO::PostgreSQL::Async::ListenManager;

{
  no warnings 'redefine';
  *DBIO::PostgreSQL::Async::ListenManager::_create_pg_connection = sub {
    my ($self) = @_;
    return MockEvPg->new(
      on_connect => sub { $self->_on_connect },
      on_notify  => sub { $self->_on_notify(@_) },
    );
  };
}

my $mgr = DBIO::PostgreSQL::Async::ListenManager->new(dsn => 'dbi:fake:');

# subscribe creates connection lazily
my @received;
$mgr->subscribe('test_channel', sub { push @received, $_[1] });

# _pg should now be set
ok $mgr->_pg, 'pg connection created on first subscribe';

# callback registered
my $cb = $mgr->_listeners->{test_channel};
ok $cb, 'listener callback stored';

# Simulate incoming notification
$mgr->_pg->fire_notify('test_channel', 'hello');
is $received[0], 'hello', 'notification dispatched to callback';

# unsubscribe clears the listener
$mgr->unsubscribe('test_channel');
ok !$mgr->_listeners->{test_channel}, 'listener removed after unsubscribe';

done_testing;
