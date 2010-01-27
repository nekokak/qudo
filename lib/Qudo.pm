package Qudo;
use strict;
use warnings;

our $VERSION = '0.02';

use Qudo::Manager;
use Carp;
use UNIVERSAL::require;
use List::Util qw/shuffle/;

our $RETRY_SECONDS = 30;
our $FIND_JOB_LIMIT_SIZE = 30;
our $DEFAULT_DRIVER = 'Skinny';
our $EXCEPTION_LIMIT_SIZE = 10;
our $EXCEPTION_OFFSET_SIZE = 0;
our $JOB_STATUS_LIMIT_SIZE = 10;
our $JOB_STATUS_OFFSET_SIZE = 0;

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
        databases           => [],
        connections         => +{},
        @_,
    }, $class;

    $self->_setup_driver;

    return $self;
}

sub _setup_driver {
    my $self = shift;

    my $driver = 'Qudo::Driver::' . $self->{driver_class};
    $driver->use or Carp::croak $@;
    $driver->init_driver($self);
}

sub set_connection {
    my ($self, $dsn, $connection) = @_;
    $self->{connections}->{$dsn} = $connection;
}
sub get_connection {
    my ($self, $dsn) = @_;
    $self->{connections}->{$dsn};
}

sub shuffled_databases {
    my $self = shift;
    my @dsns = keys %{$self->{connections}};
    return shuffle(@dsns);
}

sub driver {
    my ($self, $dsn) = @_;
    $dsn ||= $self->shuffled_databases;
    $self->driver_for($dsn);
}

sub driver_for {
    my ($self, $dsn) = @_;
    $self->get_connection($dsn);
}

sub manager {
    my $self = shift;

    $self->{manager} ||= Qudo::Manager->new(
        driver_for          => sub { $self->driver_for(+shift) },
        shuffled_databases  => sub { $self->shuffled_databases },
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

    return $self->driver->job_list($self->{find_job_limit_size}, $funcs);
}

sub job_count {
    my ($self, $funcs, $dsn) = @_;

    if ($dsn) {
        return $self->driver_for($dsn)->job_count($funcs);
    }

    my %job_count;
    for my $db ($self->shuffled_databases) {
        $job_count{$db} = $self->driver_for($db)->job_count($funcs);
    }
    return \%job_count;
}

sub exception_list {
    my ($self, $args, $dsn) = @_;

    $args->{limit}  ||= $EXCEPTION_LIMIT_SIZE;
    $args->{offset} ||= $EXCEPTION_OFFSET_SIZE;

    if ($dsn) {
        return $self->driver_for($dsn)->exception_list($args);
    }

    my %exception_list;
    for my $db ($self->shuffled_databases) {
        $exception_list{$db} = $self->driver_for($db)->exception_list($args);
    }
    return \%exception_list;
}

sub job_status_list {
    my ($self, $args, $dsn) = @_;

    $args->{limit}  ||= $JOB_STATUS_LIMIT_SIZE;
    $args->{offset} ||= $JOB_STATUS_OFFSET_SIZE;

    if ($dsn) {
        return $self->driver_for($dsn)->job_status_list($args);
    }

    my %job_status_list;
    for my $db ($self->shuffled_databases) {
        $job_status_list{$db} = $self->driver_for($db)->job_status_list($args);
    }
    return \%job_status_list;
}

=head1 NAME

Qudo - simple job queue manager

=head1 SYNOPSIS

    # enqueue job:
    use Qudo;
    my $qudo = Qudo->new(
        driver_class => 'Skinny',
        databases => [+{
            dsn      => 'dbi:SQLite:/tmp/qudo.db',
            username => '',
            password => '',
        }],
    );
    $qudo->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    
    # do work:
    use Qudo;
    my $qudo2 = Qudo->new(
        driver_class => 'Skinny',
        databases => [+{
            dsn      => 'dbi:SQLite:/tmp/qudo.db',
            username => '',
            password => '',
        }],
        manager_abilities => [qw/Worker::Test/],
    );
    $qudo2->work(); # boot manager
    # work work work!

=head1 DESCRIPTION

Qudo is simple job queue manager system.

Your application can insert job into DB ,that is managed by Qudo.
And Your application can get & execute job by Qudo worker.
Qudo corresponds to deal with DB as MySQL and SQLite.

If you add Hook Point around job's working method ,
you can add it easily and many point of work milestone.
Qudo is consided about adding Hook Point Flexibility.

=head1 REPOS

http://github.com/nekokak/qudo/tree/master

=head1 AUTHOR

Atsushi Kobayashi <nekokak _at_ gmail dot com>

Masaru Hoshino <masartz _at_ gmail dot com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;
