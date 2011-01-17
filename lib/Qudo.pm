package Qudo;
use strict;
use warnings;

our $VERSION = '0.0213';

use Qudo::Manager;
use Carp ();
use UNIVERSAL::require;
use List::Util qw/shuffle/;

our $RETRY_SECONDS = 30;
our $FIND_JOB_LIMIT_SIZE = 30;
our $DEFAULT_DRIVER = 'Skinny';
our $EXCEPTION_LIMIT_SIZE = 10;
our $EXCEPTION_OFFSET_SIZE = 0;
our $JOB_STATUS_LIMIT_SIZE = 10;
our $JOB_STATUS_OFFSET_SIZE = 0;
our $WORK_DELAY = 5;

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
        work_delay          => $WORK_DELAY,
        @_,
    }, $class;

    $self->_setup_driver;

    $self;
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
        qudo                => $self,
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
    $work_delay ||= $self->{work_delay};

    my $manager = $self->manager;
    unless ($manager->has_abilities) {
        Carp::croak 'manager dose not have abilities.';
    }

    while (1) {
        sleep $work_delay unless $manager->work_once;
    }
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

Qudo - simple and extensible job queue manager

=head1 SYNOPSIS

    # enqueue job:
    use Qudo;
    my $qudo = Qudo->new(
        driver_class => 'Skinny', # optional.
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
        driver_class => 'Skinny', # optional.
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

Qudo is simple and extensible job queue manager system.

Your application can insert job into DB ,that is managed by Qudo.
And Your application can get & execute job by Qudo worker.
Qudo corresponds to deal with DB as MySQL and SQLite.

If you add Hook Point around job's working method ,
you can add it easily and many point of work milestone.
Qudo is consided about adding Hook Point Flexibility.

=head1 USEAGE

=head2 C<Qudo-E<gt>new( %args )>

Optional members of C<%args> are:

=over 4

=item * C<driver_class>

set Qudo::Driver::(Skinny|DBI).
default driver_class is Skinny.

=back

=over 4

=item * C<databases>

An arrayref of database information. Qudo can use multiple databases,
such that if any of them are unavailable,
the worker will search for appropriate jobs in the other databases automatically.

Each member of the C<databases> value should be a hashref containing either:

=over 4

=item * C<dsn>

The database DSN for this database.

=item * C<username>

The username to use when connecting to this database.

=item * C<password>

The password to use when connecting to this database.

=back

=item * C<manager_abilities>

An arrayref of worker class name.
please specify it when moving it by the usage of worker.
it is not necessary to specify it for the usage of enqueue client.

=item * C<find_job_limit_size>

The maximum number in which it looks for job by one processing.
Qudo default limit 30.
please specify it when moving it by the usage of worker.
it is not necessary to specify it for the usage of enqueue client.

=item * C<retry_seconds>

The number of seconds after which to try reconnecting to apparently dead databases.
If not given, Qudo will retry connecting to databases after 30 seconds.

=item * C<default_hooks>

An arrayref of hook class name.

=item * C<default_plugins>

An arrayref of plugin class name.

=back

=head2 C<Qudo-E<gt>manager>

get Qudo::Manager instance.
see L<Qudo::Manager>

=head2 C<Qudo-E<gt>enqueue( %args )>

see L<Qudo::Manager> enqueue method.

=head2 C<Qudo-E<gt>work( %args )>

Find and perform any jobs $manager can do, forever.

When no job is available, the working process will sleep for $delay  seconds (or 5, if not specified) before looking again.

=head2 C<Qudo-E<gt>job_count( $funcname, $dsn )>

Returns a job count infomations.
The required arguments :

=over 4

=item * C<funcname>

the name of the function or a reference to an array of functions.

=item * C<dsn>

The database DSN for job count target database.

=back

=head2 C<Qudo-E<gt>exception_list( $args, $dsn )>

Returns a job exception infomations.
Optional members of C<$args> are:

=over 4

=item * args

=over 4

=item * limit

get exception log limit size.
default by 10.

=item * offset

get exception log offset size.
default by 0.

=back

=back

=over 4

=item * C<dsn>

The database DSN for job count target database.

=back

=head2 C<Qudo-E<gt>job_status_list( $args, $dsn )>

Returns a job exception infomations.
Optional members of C<$args> are:

=over 4

=item * args

=over 4

=item * limit

get job_status log limit size.
default by 10.

=item * offset

get job_status log offset size.
default by 0.

=back

=back

=over 4

=item * C<dsn>

The database DSN for job count target database.

=back

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

