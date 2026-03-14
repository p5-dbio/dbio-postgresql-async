package DBIO::PostgreSQL::Async;
# ABSTRACT: Async PostgreSQL storage for DBIO via EV::Pg

use strict;
use warnings;

=head1 DESCRIPTION

Async PostgreSQL support for DBIO using L<EV::Pg>, a non-blocking
PostgreSQL client built on libpq's async protocol. Bypasses DBI
entirely for maximum performance.

Supports pipeline mode (batching queries in a single round-trip),
prepared statements, COPY, and LISTEN/NOTIFY.

=head1 SYNOPSIS

  # Schema setup
  my $schema = MyApp::Schema->connect(
      'DBIO::PostgreSQL::Async',
      {
          host      => 'localhost',
          dbname    => 'myapp',
          user      => 'myapp',
          pool_size => 10,
      },
  );

  # Async queries return Futures
  $schema->resultset('Artist')->all_async->then(sub {
      my @artists = @_;
      say $_->name for @artists;
  });

  # Pipeline mode — batch queries in one round-trip
  $schema->storage->pipeline(sub {
      my @futures;
      push @futures, $schema->resultset('Artist')
          ->create_async({ name => $_ }) for @names;
      return Future->needs_all(@futures);
  });

  # LISTEN/NOTIFY
  $schema->storage->listen('changelog', sub {
      my ($channel, $payload) = @_;
      say "Event: $payload";
  });

  # Sync methods still work (block the event loop)
  my @all = $schema->resultset('Artist')->all;

=head1 EVENT LOOP COMPATIBILITY

L<EV::Pg> uses the L<EV> event loop. This works with:

=over 4

=item * L<EV> directly

=item * L<AnyEvent> (uses EV as backend when available)

=item * L<IO::Async> via L<IO::Async::Loop::EV>

=item * L<Mojolicious> via L<Mojo::Reactor::EV>

=back

=cut

sub connection {
  my ($class, $schema, $conninfo, $opts) = @_;

  require DBIO::PostgreSQL::Async::Storage;
  my $storage = DBIO::PostgreSQL::Async::Storage->new($schema);
  $storage->connect_info([$conninfo, $opts || {}]);
  return $storage;
}

1;
