package DBIO::PostgreSQL::Async::ListenManager;
# ABSTRACT: Manages LISTEN/NOTIFY subscriptions for the async PostgreSQL driver
our $VERSION = '0.900000';

use strict;
use warnings;
use EV::Pg ();
use namespace::clean;

sub new {
  my ($class, %args) = @_;
  return bless {
    dsn            => $args{dsn},
    _pg            => undef,
    _connected     => 0,
    _pending       => [],
    _listeners     => {},
  }, $class;
}

sub _pg        { $_[0]->{_pg}        }
sub _listeners { $_[0]->{_listeners} }

sub _create_pg_connection {
  my ($self) = @_;
  return EV::Pg->new(
    $self->{dsn},
    on_connect => sub { $self->_on_connect },
    on_notify  => sub { $self->_on_notify(@_) },
  );
}

sub _ensure_connected {
  my ($self) = @_;
  return if $self->{_pg};
  $self->{_pg} = $self->_create_pg_connection;
}

sub _on_connect {
  my ($self) = @_;
  $self->{_connected} = 1;
  my $pending = delete $self->{_pending} || [];
  $self->{_pending} = [];
  $self->{_pg}->query($_, sub {}) for @$pending;
}

sub _on_notify {
  my ($self, $channel, $payload) = @_;
  my $cb = $self->{_listeners}{$channel} or return;
  $cb->($channel, $payload);
}

sub _send_or_buffer {
  my ($self, $sql) = @_;
  if ($self->{_connected}) {
    $self->{_pg}->query($sql, sub {});
  } else {
    push @{ $self->{_pending} }, $sql;
  }
}

=method subscribe

    $listen_manager->subscribe($channel, $callback);

Register C<$callback> for C<$channel> and issue C<LISTEN $channel> to
PostgreSQL. Connects lazily on first call. The callback receives
C<($channel, $payload)>.

=cut

sub subscribe {
  my ($self, $channel, $callback) = @_;
  $self->_ensure_connected;
  $self->{_listeners}{$channel} = $callback;
  $self->_send_or_buffer("LISTEN $channel");
}

=method unsubscribe

    $listen_manager->unsubscribe($channel);

Remove the callback for C<$channel> and issue C<UNLISTEN $channel>.

=cut

sub unsubscribe {
  my ($self, $channel) = @_;
  delete $self->{_listeners}{$channel};
  $self->_send_or_buffer("UNLISTEN $channel") if $self->{_pg};
}

=method shutdown

    $listen_manager->shutdown;

Issue C<UNLISTEN *> and release the connection.

=cut

sub shutdown {
  my ($self) = @_;
  if ($self->{_pg}) {
    $self->_send_or_buffer('UNLISTEN *');
    $self->{_pg}        = undef;
    $self->{_connected} = 0;
  }
  %{ $self->{_listeners} } = ();
}

1;
