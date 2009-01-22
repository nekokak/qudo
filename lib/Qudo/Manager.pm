package Qudo::Manager;
use strict;
use warnings;
use Qudo::Job;

sub new {
    my ($class, %args) = @_;
    bless {%args}, $class;
}

# FIXME: driverて名前やめたいんだよなぁ
sub driver {
    my $self = shift;
    $self->{master}->driver;
}

sub enqueue {
    my ($self, $funcname, $arg, $uniqkey) = @_;

    # hook
    my $func = $self->driver->find_or_create('func',{ name => $funcname });
    # hook
    my $job = $self->driver->insert('job',
        {
            func_id => $func->id,
            arg     => $arg,
            uniqkey => $uniqkey,
        }
    );

    # hook
    return $self->lookup_job($job->id);
}

sub dequeue {
    my ($self, $job) = @_;
    $self->driver->delete('job',{id => $job->id});
}

sub work_once {
    my $self = shift;

    my $job = $self->find_job;
    return unless $job;

    my $worker_class = $job->funcname;
    return unless $worker_class;
    $worker_class->work_safely($self, $job);
}

sub lookup_job {
    my ($self, $job_id) = @_;

    my $job_itr = $self->driver->search_by_sql(q{
        SELECT
            job.id, job.arg, job.uniqkey, job.func_id,
            job.grabbed_until,
            func.name AS funcname
        FROM
            job, func
        WHERE
            job.func_id = func.id AND
            job.id      = ?
        LIMIT 1
    },[$job_id]);

    return $self->_grab_a_job($job_itr);
}

sub find_job {
    my $self = shift;

    my $job_itr = $self->driver->search_by_sql(q{
        SELECT
            job.id,  job.arg, job.uniqkey, job.func_id,
            job.grabbed_until,
            func.name AS funcname
        FROM
            job, func
        WHERE
            job.func_id = func.id
        LIMIT 10
    });

    return $self->_grab_a_job($job_itr);
}

sub _grab_a_job {
    my ($self, $job_itr) = @_;

    while (my $row = $job_itr->next) {

        my $old_grabbed_until = $row->grabbed_until;
        my $server_time = $self->get_server_time
            or die "expected a server time";

        my $worker_class = $row->funcname;
        my $grab_job = $self->driver->update('job',
            {
                grabbed_until => ($server_time + ($worker_class->grab_for || 1))
            },
            {
                id => $row->id,
                grabbed_until => $old_grabbed_until,
            }
        );
        next unless $grab_job;

        my $job = Qudo::Job->new(
            manager  => $self,
            job_data => $row,
        );
        return $job;
    }
    return;
}

sub get_server_time {
    my $self = shift;
    my $unixtime_sql = $self->driver->dbd->sql_for_unixtime;
    return $self->driver->dbh->selectrow_array("SELECT $unixtime_sql");
}

sub job_failed {
    my ($self, $job, $message) = @_;

    $self->driver->insert('exception_log',
        {
            job_id  => $job->id,
            func_id => $job->func_id,
            message => $message,
        }
    );
}

1;

