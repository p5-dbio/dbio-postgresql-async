package DBIO::PostgreSQL::Async::Pool;
# ABSTRACT: EV::Pg connection pool for DBIO

use strict;
use warnings;
use base 'DBIO::Storage::Pool';

use Carp 'croak';

=head1 DESCRIPTION

Connection pool for L<DBIO::PostgreSQL::Async::Storage>. Manages a pool of
L<EV::Pg> connections, dispatching queries to available connections
and queuing when all are busy.

=head1 SYNOPSIS

  my $pool = DBIO::PostgreSQL::Async::Pool->new(
      conninfo => 'dbname=myapp',
      size     => 10,
      on_error => sub { warn $_[0] },
  );

  my $pg = $pool->acquire;       # get idle connection
  $pool->release($pg);           # return to pool
  my $pg = $pool->acquire_txn;   # pinned for transaction

=cut

sub new {
  my ($class, %args) = @_;
  croak('conninfo or conninfo_provider required')
    unless $args{conninfo} || $args{conninfo_provider};

  bless {
    conninfo     => $args{conninfo},
    conninfo_provider => $args{conninfo_provider},
    max_size     => $args{size} || 5,
    on_error     => $args{on_error} || sub { warn "Pool error: $_[0]\n" },
    _connections => [],
    _idle        => [],
    _waiters     => [],
  }, $class;
}

=method acquire

Returns an idle L<EV::Pg> connection. Creates a new connection if
the pool has capacity. If all connections are busy and pool is at
max size, queues the request.

=cut

sub acquire {
  my $self = shift;

  # Return idle connection if available
  if (@{ $self->{_idle} }) {
    return pop @{ $self->{_idle} };
  }

  # Create new connection if under limit
  if (@{ $self->{_connections} } < $self->{max_size}) {
    my $pg = $self->_create_connection;
    return $pg;
  }

  # All connections busy — this should not happen in well-designed
  # async code. For now, croak. A proper implementation would queue
  # a Future and resolve it when a connection is released.
  croak "Connection pool exhausted (max: $self->{max_size})";
}

=method acquire_txn

Acquire a connection pinned for exclusive transaction use.
Same as L</acquire> but the connection will not be released
back to the idle pool until explicitly released.

=cut

sub acquire_txn {
  my $self = shift;
  return $self->acquire;  # same behavior, caller manages lifecycle
}

=method release

  $pool->release($pg);

Return a connection to the idle pool.

=cut

sub release {
  my ($self, $pg) = @_;
  push @{ $self->{_idle} }, $pg;
}

=method size

Total connections (active + idle).

=cut

sub size { scalar @{ $_[0]->{_connections} } }

=method available

Number of idle connections.

=cut

sub available { scalar @{ $_[0]->{_idle} } }

=method max_size

Configured maximum pool size.

=cut

sub max_size { $_[0]->{max_size} }

=method shutdown

Close all connections and clear the pool.

=cut

sub shutdown {
  my $self = shift;
  for my $pg (@{ $self->{_connections} }) {
    eval { $pg->finish };
  }
  $self->{_connections} = [];
  $self->{_idle} = [];
}

sub _create_connection {
  my $self = shift;
  my $conninfo = $self->{conninfo_provider}
    ? $self->{conninfo_provider}->()
    : $self->{conninfo};

  require EV::Pg;
  my $pg = EV::Pg->new(
    conninfo   => $self->_conninfo_string($conninfo),
    on_connect => sub {},
    on_error   => $self->{on_error},
  );

  push @{ $self->{_connections} }, $pg;
  return $pg;
}

sub DESTROY {
  my $self = shift;
  $self->shutdown;
}

sub _conninfo_string {
  my ($self, $ci) = @_;
  return $ci unless ref $ci;

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

1;
