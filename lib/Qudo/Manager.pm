package Qudo::Manager;
use strict;
use warnings;
use Qudo::Job;
use Carp;

sub new {
    my ($class, %args) = @_;
    bless {%args}, $class;
}

sub driver {
    my $self = shift;
    $self->{master}->driver;
}

sub call_hook {
    my ($self, $hook_point, $args) = @_;
    $self->{master}->call_hook($hook_point, $args);
}

sub enqueue {
    my ($self, $funcname, $arg, $uniqkey) = @_;

    my $func_id = $self->driver->get_func_id( $funcname );

    unless ($func_id) {
        croak "$funcname can't get";
    }

    my $args = +{
        func_id => $func_id,
        arg     => $arg,
        uniqkey => $uniqkey,
    };
    $self->call_hook('pre_enqueue', $args);

    my $job_id = $self->driver->enqueue($args);

    return $self->lookup_job($job_id);
}

sub dequeue {
    my ($self, $job) = @_;
    $self->driver->dequeue({id => $job->id});
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
        my $server_time = $self->driver->get_server_time
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

sub job_failed {
    my ($self, $job, $message) = @_;

    $self->driver->logging_exception(
        {
            job_id  => $job->id,
            func_id => $job->func_id,
            message => $message,
        }
    );
}

1;

