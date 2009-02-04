package Qudo;
use strict;
use warnings;

our $VERSION = '0.01';

use Qudo::Model;
use Qudo::Manager;
use Carp;
use UNIVERSAL::require;

our $RETRY_SECONDS = 30;
our $FIND_JOB_LIMIT_SIZE = 30;

sub new {
    my ($class, %args) = @_;

    my $self = bless {}, $class;

    croak "database must be an hashref if specified"
        unless !exists $args{database} || ref $args{database} eq 'HASH';
    my $database = delete $args{database};

    $self->{retry_seconds} = delete $args{retry_seconds} || $RETRY_SECONDS;
    $self->{find_job_limit_size} = delete $args{find_job_limit_size} || $FIND_JOB_LIMIT_SIZE;
    $self->{hooks} = +{};

    Qudo::Model->reconnect($database);

    return $self;
}

sub manager {
    my $self = shift;
    Qudo::Manager->new(
        master => $self,
    );
}

sub driver { 'Qudo::Model' }

sub call_hook {
    my ($self, $hook_point, $args) = @_;

    for my $module (keys %{$self->{hooks}->{$hook_point}}) {
        my $code = $self->{hooks}->{$hook_point}->{$module};
        $code->($args);
    }
}

sub register_hook {
    my ($self, @hook_modules) = @_;

    for my $module (@hook_modules) {
        $module->require or croak $@;
        my ($hook_point, $code) = $module->load();
        $self->{hooks}->{$hook_point}->{$module} = $code;
    }
}

sub unregister_hook {
    my ($self, @hook_modules) = @_;

    for my $module (@hook_modules) {
        my $hook_point = $module->unload();
        delete $self->{hooks}->{$hook_point}->{$module};
    }
}

=head1 NAME

Qudo - simple job queue manager

=head1 SYNOPSIS

  use Qudo;

=head1 DESCRIPTION

simple job queue manager.

=head1 AUTHOR

Atsushi Kobayashi <nekokak _at_ gmail dot com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;
