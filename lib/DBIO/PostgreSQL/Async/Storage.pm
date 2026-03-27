package DBIO::PostgreSQL::Async::Storage;
# ABSTRACT: Async PostgreSQL storage driver using EV::Pg

use strict;
use warnings;
use base 'DBIO::Storage::Async';

use Carp 'croak';
use Future;
use Scalar::Util 'blessed';
use DBIO::SQLMaker;
use namespace::clean;

=head1 DESCRIPTION

Implements L<DBIO::Storage::Async> using L<EV::Pg> — a non-blocking
PostgreSQL client that speaks libpq's async protocol directly.
No DBI, no DBD::Pg, just raw libpq performance.

Features:

=over 4

=item * Pipeline mode — batch queries in a single network round-trip

=item * Prepared statement caching

=item * LISTEN/NOTIFY for real-time event streaming

=item * COPY IN/OUT for bulk data transfer

=item * Connection pooling with transaction pinning

=back

=cut

my $PREPARED_COUNTER = 0;

sub new {
  my ($class, $schema, $args) = @_;
  my $self = bless {
    schema          => $schema,
    pool            => undef,
    connect_info    => undef,
    sql_maker       => undef,
    sql_maker_class => 'DBIO::SQLMaker',
    _prepared_cache => {},
    _listeners      => {},
    _conninfo_provider => undef,
    debug           => $ENV{DBIO_TRACE} || 0,
    debugobj        => undef,
  }, $class;
  $self;
}

=method future_class

Returns C<'Future'> — uses L<Future.pm|Future> from CPAN.

=cut

sub future_class { 'Future' }

=method connect_info

  $storage->connect_info([ \%conninfo, \%opts ]);

Set connection parameters. C<%conninfo> is passed directly to
L<EV::Pg> as libpq connection parameters (host, dbname, user, etc.).

=cut

sub connect_info {
  my ($self, $info) = @_;
  if ($info) {
    $self->disconnect if $self->{pool} || $self->{_listen_pg};
    $self->{connect_info} = $info;

    if (ref $info eq 'ARRAY' && @$info == 1 && blessed($info->[0])
        && $info->[0]->isa('DBIO::AccessBroker')) {
      $self->set_access_broker($info->[0], 'write');
      $self->{_conninfo_provider} = sub {
        my ($conninfo) = $self->_normalize_async_connect_info(
          $self->_current_async_connect_info($self->access_broker_mode)
        );
        return $conninfo;
      };
      my ($conninfo, $pool_size, $opts) = $self->_normalize_async_connect_info(
        $self->_current_async_connect_info($self->access_broker_mode)
      );
      $self->{_conninfo}  = $conninfo;
      $self->{_pool_size} = $pool_size;
      $self->{_opts}      = $opts;
    }
    else {
      $self->clear_access_broker;
      $self->{_conninfo_provider} = undef;
      my ($conninfo, $pool_size, $opts) = $self->_normalize_async_connect_info($info);
      $self->{_conninfo}  = $conninfo;
      $self->{_pool_size} = $pool_size;
      $self->{_opts}      = $opts;
    }
  }
  return $self->{connect_info};
}

sub _current_async_connect_info {
  my ($self, $mode) = @_;

  my $connect_info = $self->current_access_broker_connect_info($mode);
  return $connect_info if $connect_info;

  return [ $self->{_conninfo}, $self->{_opts} || {} ];
}

sub _normalize_async_connect_info {
  my ($self, $info) = @_;

  my $conninfo = $info->[0];
  $conninfo = ref($conninfo) eq 'HASH' ? { %$conninfo } : $conninfo;

  my $opts = $info->[1];
  $opts = ref($opts) eq 'HASH' ? { %$opts } : {};

  my $pool_size = 5;
  if (ref($conninfo) eq 'HASH') {
    $pool_size = delete $conninfo->{pool_size} || 5;
  }

  return ($conninfo, $pool_size, $opts);
}

sub _conninfo_provider { $_[0]->{_conninfo_provider} }

=method pool

Returns the L<DBIO::PostgreSQL::Async::Pool> connection pool.
Created lazily on first access.

=cut

sub pool {
  my $self = shift;
  $self->{pool} ||= do {
    require DBIO::PostgreSQL::Async::Pool;
    my %args = (
      size     => $self->{_pool_size},
      on_error => sub { warn "DBIO::PostgreSQL::Async pool error: $_[0]\n" },
    );

    if ($self->{_conninfo_provider}) {
      $args{conninfo_provider} = $self->{_conninfo_provider};
    }
    else {
      $args{conninfo} = $self->_conninfo_string;
    }

    DBIO::PostgreSQL::Async::Pool->new(%args);
  };
}

sub _conninfo_string {
  my ($self, $ci) = @_;
  $ci = $self->_current_async_connect_info($self->access_broker_mode)->[0]
    if ! defined $ci;
  return $ci unless ref $ci;
  # Convert hashref to libpq conninfo string
  return join(' ', map { "$_=" . _escape_conninfo($ci->{$_}) }
    grep { defined $ci->{$_} } keys %$ci);
}

sub _escape_conninfo {
  my $val = shift;
  return "''" unless defined $val && length $val;
  return $val unless $val =~ /[\s'\\]/;
  $val =~ s/\\/\\\\/g;
  $val =~ s/'/\\'/g;
  return "'$val'";
}

# --- SQL Generation ---

=method sql_maker

Returns the L<DBIO::SQLMaker> instance, configured for PostgreSQL
(double-quote quoting, LIMIT/OFFSET dialect).

=cut

sub sql_maker {
  my $self = shift;
  $self->{sql_maker} ||= do {
    my $class = $self->{sql_maker_class};
    $class->new(
      quote_char     => '"',
      name_sep       => '.',
      limit_dialect  => 'LimitOffset',
    );
  };
}

sub _generate_sql {
  my ($self, $op, @args) = @_;
  my $sm = $self->sql_maker;
  my $method = {
    select        => 'select',
    select_single => 'select',
    insert        => 'insert',
    update        => 'update',
    delete        => 'delete',
  }->{$op} or croak "Unknown operation: $op";

  return $sm->$method(@args);
}

# --- Async Query Execution ---

=method select_async

  my $future = $storage->select_async($source, $select, $where, $attrs);

Execute a SELECT query asynchronously. Returns a L<Future> that
resolves with the result rows (arrayrefs).

=cut

sub select_async {
  my ($self, $ident, $select, $condition, $attrs) = @_;

  my ($sql, @bind) = $self->sql_maker->select($ident, $select, $condition, $attrs);
  $self->_query_async($sql, \@bind);
}

=method select_single_async

Like L</select_async> but returns only the first row.

=cut

sub select_single_async {
  my ($self, $ident, $select, $condition, $attrs) = @_;

  my ($sql, @bind) = $self->sql_maker->select($ident, $select, $condition, $attrs);
  $self->_query_async($sql, \@bind)->then(sub {
    my @rows = @_;
    return @rows ? $rows[0] : undef;
  });
}

=method insert_async

  my $future = $storage->insert_async($source, \%vals);

=cut

sub insert_async {
  my ($self, $source, $to_insert) = @_;

  my ($sql, @bind) = $self->sql_maker->insert($source, $to_insert);
  # PostgreSQL RETURNING for auto-generated columns
  $sql .= ' RETURNING *' unless $sql =~ /RETURNING/i;
  $self->_query_async($sql, \@bind);
}

=method update_async

  my $future = $storage->update_async($source, \%vals, \%where);

=cut

sub update_async {
  my ($self, $source, $fields, $where) = @_;

  my ($sql, @bind) = $self->sql_maker->update($source, $fields, $where);
  $self->_query_async($sql, \@bind);
}

=method delete_async

  my $future = $storage->delete_async($source, \%where);

=cut

sub delete_async {
  my ($self, $source, $where) = @_;

  my ($sql, @bind) = $self->sql_maker->delete($source, $where);
  $self->_query_async($sql, \@bind);
}

# Low-level async query dispatch

sub _query_async {
  my ($self, $sql, $bind) = @_;
  $bind ||= [];

  $self->_debug_query($sql, $bind) if $self->{debug};

  my $pg = $self->pool->acquire;
  my $f = Future->new;

  $pg->query_params($sql, $bind, sub {
    my ($rows, $err) = @_;
    if ($err) {
      $f->fail($err);
    } else {
      $f->done(ref $rows eq 'ARRAY' ? @$rows : $rows);
    }
    $self->pool->release($pg) unless $self->{_in_txn};
  });

  return $f;
}

sub _debug_query {
  my ($self, $sql, $bind) = @_;
  my $bind_str = join(', ', map { defined $_ ? "'$_'" : 'NULL' } @$bind);
  warn "$sql: $bind_str\n";
}

# --- Transactions ---

=method txn_do_async

  my $future = $storage->txn_do_async(sub {
      my ($storage) = @_;
      # All queries in here use the same connection
      $storage->insert_async(...)->then(sub { ... });
  });

Acquires a connection from the pool, issues BEGIN, executes the
coderef, and issues COMMIT on success or ROLLBACK on Future failure.

=cut

sub txn_do_async {
  my ($self, $coderef, @args) = @_;

  my $pg = $self->pool->acquire_txn;
  my $txn_storage = bless {
    %$self,
    _txn_pg  => $pg,
    _in_txn  => 1,
  }, ref($self);

  my $f = Future->new;

  $pg->query('BEGIN', sub {
    my (undef, $err) = @_;
    if ($err) {
      $self->pool->release($pg);
      $f->fail("BEGIN failed: $err");
      return;
    }

    my $inner = eval { $coderef->($txn_storage, @args) };
    if ($@) {
      my $error = $@;
      $pg->query('ROLLBACK', sub {
        $self->pool->release($pg);
        $f->fail($error);
      });
      return;
    }

    # If coderef returned a Future, chain COMMIT/ROLLBACK
    if (ref $inner && $inner->can('then')) {
      $inner->then(sub {
        my @result = @_;
        my $commit_f = Future->new;
        $pg->query('COMMIT', sub {
          my (undef, $cerr) = @_;
          $self->pool->release($pg);
          if ($cerr) {
            $commit_f->fail("COMMIT failed: $cerr");
          } else {
            $commit_f->done(@result);
          }
        });
        return $commit_f;
      })->catch(sub {
        my $error = shift;
        my $rb_f = Future->new;
        $pg->query('ROLLBACK', sub {
          $self->pool->release($pg);
          $rb_f->fail($error);
        });
        return $rb_f;
      })->on_done(sub { $f->done(@_) })
        ->on_fail(sub { $f->fail(@_) });
    } else {
      # Coderef returned a plain value — commit immediately
      $pg->query('COMMIT', sub {
        my (undef, $cerr) = @_;
        $self->pool->release($pg);
        if ($cerr) {
          $f->fail("COMMIT failed: $cerr");
        } else {
          $f->done($inner);
        }
      });
    }
  });

  return $f;
}

# --- Pipeline Mode ---

=method pipeline

  my $future = $storage->pipeline(sub {
      my ($storage) = @_;
      my @futures;
      push @futures, $storage->insert_async('artist', { name => $_ })
          for @names;
      return Future->needs_all(@futures);
  });

Execute multiple queries in pipeline mode. All queries are batched
and sent in a single network round-trip for maximum throughput.

=cut

sub pipeline {
  my ($self, $coderef) = @_;

  my $pg = $self->pool->acquire;
  $pg->enter_pipeline;

  my $result = eval { $coderef->($self) };
  my $err = $@;

  if ($err) {
    $pg->exit_pipeline;
    $self->pool->release($pg);
    return Future->fail($err);
  }

  # Sync the pipeline — callback fires when all results are in
  my $f = Future->new;
  $pg->pipeline_sync(sub {
    $pg->exit_pipeline;
    $self->pool->release($pg);
    if (ref $result && $result->can('then')) {
      $result->on_done(sub { $f->done(@_) })
             ->on_fail(sub { $f->fail(@_) });
    } else {
      $f->done($result);
    }
  });

  return $f;
}

# --- LISTEN/NOTIFY ---

=method listen

  $storage->listen($channel, sub {
      my ($channel, $payload, $sender_pid) = @_;
      # Handle notification
  });

Subscribe to PostgreSQL LISTEN/NOTIFY notifications on the given
channel. The callback fires each time a notification arrives.

=cut

sub listen {
  my ($self, $channel, $cb) = @_;

  $self->{_listeners}{$channel} = $cb;

  # Use a dedicated connection for LISTEN (not from the pool)
  $self->{_listen_pg} ||= do {
    require EV::Pg;
    my $pg = EV::Pg->new(
      conninfo   => $self->_conninfo_string,
      keep_alive => 1,
      on_connect => sub {},
      on_error   => sub { warn "LISTEN connection error: $_[0]\n" },
      on_notify  => sub {
        my ($ch, $payload, $pid) = @_;
        if (my $handler = $self->{_listeners}{$ch}) {
          $handler->($ch, $payload, $pid);
        }
      },
    );
    $pg;
  };

  my $quoted = $self->sql_maker->_quote($channel);
  $self->{_listen_pg}->query("LISTEN $quoted", sub {});
}

=method unlisten

  $storage->unlisten($channel);

Unsubscribe from a notification channel.

=cut

sub unlisten {
  my ($self, $channel) = @_;
  delete $self->{_listeners}{$channel};
  if ($self->{_listen_pg}) {
    my $quoted = $self->sql_maker->_quote($channel);
    $self->{_listen_pg}->query("UNLISTEN $quoted", sub {});
  }
}

# --- COPY ---

=method copy_in

  $storage->copy_in($table, \@columns, sub {
      my ($put) = @_;
      $put->(['Miles Davis', 'Jazz']);
      $put->(['John Coltrane', 'Jazz']);
  });

Bulk load data via PostgreSQL COPY FROM STDIN. The callback receives
a writer function that accepts arrayrefs of column values.

=cut

sub copy_in {
  my ($self, $table, $columns, $coderef) = @_;

  my $col_list = join(', ', map { $self->sql_maker->_quote($_) } @$columns);
  my $quoted_table = $self->sql_maker->_quote($table);
  my $sql = "COPY $quoted_table ($col_list) FROM STDIN";

  my $pg = $self->pool->acquire;
  my $f = Future->new;

  $pg->query($sql, sub {
    my ($status, $err) = @_;
    if ($err) {
      $self->pool->release($pg);
      $f->fail($err);
      return;
    }

    my $put = sub {
      my $row = shift;
      my $line = join("\t", map { defined $_ ? $_ : '\N' } @$row) . "\n";
      $pg->put_copy_data($line);
    };

    eval { $coderef->($put) };
    if ($@) {
      $pg->put_copy_end($@);
      $self->pool->release($pg);
      $f->fail($@);
    } else {
      $pg->put_copy_end;
      $self->pool->release($pg);
      $f->done(1);
    }
  });

  return $f;
}

# --- Sync Fallbacks ---
# These allow sync methods (->all, ->first etc.) to work
# by blocking the event loop. Useful for scripts/migrations.

sub select {
  my $self = shift;
  return $self->select_async(@_)->get;
}

sub select_single {
  my $self = shift;
  return $self->select_single_async(@_)->get;
}

sub insert {
  my $self = shift;
  return $self->insert_async(@_)->get;
}

sub update {
  my $self = shift;
  return $self->update_async(@_)->get;
}

sub delete {
  my $self = shift;
  return $self->delete_async(@_)->get;
}

sub txn_do {
  my $self = shift;
  return $self->txn_do_async(@_)->get;
}

# --- Schema Integration ---

sub schema { $_[0]->{schema} }
sub debug  { $_[0]->{debug} }

sub connected { defined $_[0]->{pool} && $_[0]->pool->available > 0 }

sub disconnect {
  my $self = shift;
  if ($self->{pool}) {
    $self->{pool}->shutdown;
    $self->{pool} = undef;
  }
  if ($self->{_listen_pg}) {
    $self->{_listen_pg}->finish;
    $self->{_listen_pg} = undef;
  }
}

sub DESTROY {
  my $self = shift;
  $self->disconnect if $self->{pool};
}

1;
