package Qudo::Job;
use strict;
use warnings;

sub new {
    my ($class, %args) = @_;
    bless {%args}, $class;
}

sub id       { $_[0]->{job_data}->{job_id}       }
sub uniqkey  { $_[0]->{job_data}->{job_uniqkey}  }
sub func_id  { $_[0]->{job_data}->{func_id}      }

sub funcname {
    my $self = shift;
    $self->manager->funcid_to_name($self->func_id, $self->db);
}

sub retry_cnt     { $_[0]->{job_data}->{job_retry_cnt}     }
sub grabbed_until { $_[0]->{job_data}->{job_grabbed_until} }
sub priority { $_[0]->{job_data}->{job_priority} }
sub arg : lvalue  { $_[0]->{job_data}->{job_arg}           }
sub arg_origin : lvalue { $_[0]->{arg_origin} }
sub db { $_[0]->{db} }

sub manager  { $_[0]->{manager} }
sub job_start_time : lvalue { $_[0]->{job_start_time} }

sub completed {
    my $self = shift;

    $self->{_complete} = 1;

    return unless $self->funcname->set_job_status;
    $self->manager->set_job_status($self, 'completed');
}

sub is_completed { $_[0]->{_complete} }
sub is_aborted   { $_[0]->{_abort}    }
sub is_failed    { $_[0]->{_fail}    }

sub reenqueue {
    my ($self, $args) = @_;
    $self->manager->reenqueue($self, $args);
}

sub dequeue {
    my $self = shift;
    $self->manager->dequeue($self);
}

sub error {
    my ($self, ) = @_;
    return $self->{_error}
}

sub failed {
    my ($self, $error) = @_;

    $self->{_fail} = 1;
    $self->{_error} = $error;

    if ($self->funcname->set_job_status) {
        $self->manager->set_job_status($self, 'failed');
    }
    $self->manager->job_failed($self, $error);
}

sub abort {
    my ($self, $error) = @_;

    $self->{_abort} = 1;
    $error ||= 'abort!!';
    $self->{_error} = $error;

    if ($self->funcname->set_job_status) {
        $self->manager->set_job_status($self, 'abort');
    }
    $self->manager->job_failed($self, $error);
}

sub replace {
    my ($self, @jobs) = @_;

    my $db = $self->manager->driver_for($self->db);
    $db->dbh->begin_work;

        for my $job (@jobs) {
            $self->manager->enqueue(@$job, $self->db);
        }

        $self->completed;

    $db->dbh->commit;
}

=head1 NAME

Qudo::Job - Qudo job class

=head1 SYNOPSIS

  # You don't need to create job object by yourself.

=head1 DESCRIPTION

Qudo::Job object is passed to your worker and some hook points.

=head1 METHODS

=head2  id

Returns job id.

=head2  uniqkey

Returns the job unique key.

=head2  func_id

Returns the function id of the job.

=head2  funcname

Returns the function name of the job.

=head2 retry_cnt

Returns how many times the job is retried.

=head2 grabbed_until

Returns time when job is grabbed.

=head2  priority

Returns the priority of the job.

=head2  arg

Returns the job argument.

=head2  arg_origin

Returns the original argument before a serializer change.

=head2  db

Returns the database the job belonging.

=head2  manager

Returns Qudo manager.

=head2  job_start_time

Returns time when job started.

=head2  completed

Set job as completed successfully.

=head2  is_completed

If job is completed, returns true.

=head2  is_aborted

If job is aborted, returns true.

=head2  is_failed

If job is failed, returns true.

=head2  error

Returns error message set in failed or abort

=head2  failed

 $job->failed($reason);

Don't use this method in your worker class.
Use die instead of this.
$reason is set as error and logged in exception_log.

=head2  abort

 $job->abort($reason);

This aborts job.
When this method is called, the job never retried even if you set retry_cnt is set.
But, if you override work_safely in Qudo::Worker, it is depends on your implementation.
$reason is set as error and  logged in exception_log.

=head2  replace

 $job->replace(['Worker::One', {arg => 'arg1'}], ['Worker::Another', {arg => 'arg2'}]);

This enqueue new job(s) and current job itself is completed.

=cut

1;
