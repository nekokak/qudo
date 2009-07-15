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
our $EXCEPTION_LIMIT_SIZE = 10;
our $EXCEPTION_OFFSET_SIZE = 0;

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

sub enqueue {
    my $self = shift;
    $self->manager->enqueue(@_);
}

sub work {
    my ($self, $work_delay) = @_;
    $work_delay ||= 5;

    my $manager = $self->manager;
    unless ($manager->has_abilities) {
        Carp::croak 'manager dose not have abilities.';
    }

    while (1) {
        sleep $work_delay unless $manager->work_once;
    }
}

sub job_list {
    my ($self, $funcs) = @_;

    return $self->{driver}->job_list($self->{find_job_limit_size}, $funcs);
}

sub job_count {
    my ($self, $funcs) = @_;

    return $self->{driver}->job_count($funcs);
}

sub exception_list {
    my ($self, %args) = @_;

    $args{limit}  ||= $EXCEPTION_LIMIT_SIZE;
    $args{offset} ||= $EXCEPTION_OFFSET_SIZE;
    return $self->{driver}->exception_list(%args);
}

=head1 NAME

Qudo - simple job queue manager

=head1 SYNOPSIS

    # enqueue job:
    use Qudo;
    my $qudo = Qudo->new(
        driver_class => 'Skinny',
        database => +{
            dsn      => 'dbi:SQLite:/tmp/qudo.db',
            username => '',
            password => '',
        },
    );
    $qudo->enqueue("Worker::Test", 'arg', 'uniqkey');
    
    # do work:
    use Qudo;
    my $qudo2 = Qudo->new(
        driver_class => 'Skinny',
        database => +{
            dsn      => 'dbi:SQLite:/tmp/qudo.db',
            username => '',
            password => '',
        },
        manager_abilities => [qw/Worker::Test/],
    );
    $qudo2->work(); # boot manager
    # work work work!

=head1 DESCRIPTION

Qudo is simple job queue manager system.

Your application can insert job into DB ,that is managed by Qudo.
And Your application can get & execute job by Qudo worker.
Qudo corresponds to deal with DB as Mysql and SQLite.

If you add Hook Point around job's working method ,
you can add it easily and many point of work milestone.
Qudo is consided about adding Hook Point Flexibility.

=head1 REPOS

http://github.com/nekokak/qudo/tree/master

=head1 AUTHOR

Atsushi Kobayashi <nekokak _at_ gmail dot com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;
