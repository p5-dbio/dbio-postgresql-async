use strict;
use warnings;
use Test::More;

BEGIN { eval { require Future; 1 } or plan skip_all => 'Future not installed' }

use DBIO::PostgreSQL::Async::TransactionContext;

# Minimal mock storage and connection
{
  package MockStorage27;
  sub new { bless { pool => MockPool27->new }, $_[0] }
  sub pool { $_[0]->{pool} }
  sub _query_async {
    my ($self, $sql, $bind) = @_;
    push @{ $self->{_queries} //= [] }, $sql;
    return Future->done([]);
  }
  sub _query_async_on {
    my ($self, $pg, $sql, $bind) = @_;
    push @{ $self->{_queries} //= [] }, $sql;
    return Future->done([]);
  }
}
{
  package MockPool27;
  sub new { bless {}, $_[0] }
  sub release { }
}
{
  package MockConn27;
  sub new { bless {}, $_[0] }
}

my $storage = MockStorage27->new;
my $pg      = MockConn27->new;
my $ctx     = DBIO::PostgreSQL::Async::TransactionContext->new(
  storage => $storage,
  pg      => $pg,
);

isa_ok $ctx, 'DBIO::PostgreSQL::Async::TransactionContext';

# in_txn is always true
ok $ctx->in_txn, 'in_txn returns true';

# txn_pg returns the pinned connection
is $ctx->txn_pg, $pg, 'txn_pg returns the connection';

# pool delegates to storage
is $ctx->pool, $storage->pool, 'pool delegates to underlying storage';

# query methods forward to storage
my $f = $ctx->_query_async('SELECT 1', []);
isa_ok $f, 'Future';

done_testing;
