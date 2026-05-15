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

=attr storage

The parent L<DBIO::PostgreSQL::Async::Storage> instance.

=cut

sub storage { $_[0]->{storage} }

=attr pg

The pinned L<EV::Pg> connection handle for the duration of this transaction.

=cut

sub txn_pg  { $_[0]->{pg}      }

=attr pool

Shortcut to C<< $self->storage->pool >>.

=cut

sub pool    { $_[0]->{storage}->pool }

=attr in_txn

Always true — indicates we are inside a transaction.

=cut

sub in_txn  { 1 }

=method _query_async

Executes a query on the pinned transaction connection without releasing
it back to the pool. Uses L<DBIO::PostgreSQL::Async::Storage/_query_async_pinned>.

=cut

sub _query_async {
  my ($self, $sql, $bind) = @_;
  return $self->{storage}->_query_async_pinned($self->{pg}, $sql, $bind);
}

# Explicitly delegate the full public API to storage for backward compatibility.
# These are the methods a txn_do_async callback is expected to call.
for my $method (qw(
  select_async select_single_async insert_async update_async delete_async
  select select_single insert update delete
  sql_maker debug
)) {
  eval "sub $method { my \$self = shift; \$self->{storage}->$method(\@_) }"
    or die "TransactionContext delegation failed for $method: $@";
}

1;
