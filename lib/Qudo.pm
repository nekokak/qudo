package Qudo;
use strict;
use warnings;

our $VERSION = '0.01';

use Qudo::Manager;
use Carp;
use UNIVERSAL::require;

our $RETRY_SECONDS = 30;
our $FIND_JOB_LIMIT_SIZE = 30;
our $DEFAULT_DRIVER = 'Skinny';

sub new {
    my $class = shift;

    my $self = bless {
        retry_seconds       => $RETRY_SECONDS,
        find_job_limit_size => $FIND_JOB_LIMIT_SIZE,
        driver_class        => $DEFAULT_DRIVER,
        hooks               => +{},
        @_,
    }, $class;

    $self->setup_driver;

    return $self;
}

sub setup_driver {
    my $self = shift;

    my $driver = 'Qudo::Driver::' . $self->{driver_class};
    $driver->use or die $@;
    $self->{driver} = $driver->init_driver($self);
}

sub manager {
    my $self = shift;
    Qudo::Manager->new(
        master              => $self,
        driver              => $self->{driver},
        find_job_limit_size => $self->{find_job_limit_size},
        retry_seconds       => $self->{retry_seconds},
    );
}

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
