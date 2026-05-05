package DBIO::PostgreSQL::Async::TransactionContext;
# ABSTRACT: Pinned connection context for an async PostgreSQL transaction
our $VERSION = '0.900000';

use strict;
use warnings;
use Future ();
use namespace::clean;

sub new {
  my ($class, %args) = @_;
  return bless {
    storage => $args{storage} // die('storage required'),
    pg      => $args{pg}      // die('pg required'),
  }, $class;
}

sub storage { $_[0]->{storage} }
sub txn_pg  { $_[0]->{pg}      }
sub pool    { $_[0]->{storage}->pool }
sub in_txn  { 1 }

=method _query_async

Forwards query execution to the underlying storage using the pinned
transaction connection (C<txn_pg>).

=cut

sub _query_async {
  my ($self, $sql, $bind) = @_;
  # The storage's _query_async needs to know which pg to use.
  # We add a variant on storage: _query_async_on($pg, $sql, $bind)
  # For now, forward to storage._query_async — the storage's txn_do_async
  # will be refactored to use _query_async_on(pg, ...) via txn_pg.
  return $self->{storage}->_query_async($sql, $bind, $self->{pg});
}

# Forward everything else to the wrapped storage via AUTOLOAD
our $AUTOLOAD;
sub AUTOLOAD {
  my $self = shift;
  my $method = $AUTOLOAD;
  $method =~ s/.*:://;
  return if $method eq 'DESTROY';
  return $self->{storage}->$method(@_);
}

1;
