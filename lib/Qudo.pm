package Qudo;
use strict;
use warnings;

our $VERSION = '0.01';

use Qudo::Manager;
use Carp;

our $RETRY_SECONDS = 30;
our $FIND_JOB_LIMIT_SIZE = 30;
our $DEFAULT_DRIVER = 'Skinny';

sub new {
    my $class = shift;

    my $self = bless {
        retry_seconds       => $RETRY_SECONDS,
        find_job_limit_size => $FIND_JOB_LIMIT_SIZE,
        driver_class        => $DEFAULT_DRIVER,
        default_hooks       => [],
        default_plugins     => [],
        manager             => '',
        manager_abilities   => [],
        @_,
    }, $class;

    $self->setup_driver;

    return $self;
}

sub setup_driver {
    my $self = shift;

    my $driver = 'Qudo::Driver::' . $self->{driver_class};
    $driver->use or Carp::croak $@;
    $self->{driver} = $driver->init_driver($self);
}

sub manager {
    my $self = shift;

    $self->{manager} ||= Qudo::Manager->new(
        driver              => $self->{driver},
        find_job_limit_size => $self->{find_job_limit_size},
        retry_seconds       => $self->{retry_seconds},
        default_hooks       => $self->{default_hooks},
        default_plugins     => $self->{default_plugins},
        abilities           => $self->{manager_abilities},
    );
}

sub work {
    my ($self, $work_delay) = @_;
    $work_delay ||= 5;

    my $manager = $self->manager;
    unless ($manager->has_abilities) {
        Carp::croak 'manager dose not have abilities.';
    }

    while (1) {
        sleep $work_delay unless $self->manager->work_once;
    }
}

=head1 NAME

Qudo - simple job queue manager

=head1 SYNOPSIS

    # enqueue job:
    use Qudo;
    my $qudo = Qudo->new(
        driver_class => 'Skinny',
        database => +{
            dsn      => 'dbi:sqlite:/tmp/qudo.db',
            username => '',
            password => '',
        }
    );
    $qudo->manager->enqueue("Worker::Test", 'arg', 'uniqkey');
    
    # do work:
    use Qudo;
    my $qudo = Qudo->new(
        driver_class => 'Skinny',
        database => +{
            dsn      => 'dbi:sqlite:/tmp/qudo.db',
            username => '',
            password => '',
        }
        manager_abilities => [qw/Worker::Test/],
    );
    $qudo->work(); # boot manager
    # work work work!

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
