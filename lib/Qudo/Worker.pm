package Qudo::Worker;
use strict;
use warnings;
use base 'Class::Data::Inheritable';
use Qudo::HookLoader;

__PACKAGE__->mk_classdata(qw/_hooks/);

sub max_retries { 0 }
sub retry_delay { 0 }
sub grab_for    { 60*60 } # default setting 1 hour
sub set_job_status { 0 }  # job process status store for job_status table.
sub hooks {
    my $class = shift;
    $class->_hooks(+{}) unless $class->_hooks;
    $class->_hooks;
}

sub register_hooks {
    my ($class, @hook_modules) = @_;
    Qudo::HookLoader->register_hooks($class, \@hook_modules);
}

sub unregister_hooks {
    my ($class, @hook_modules) = @_;
    Qudo::HookLoader->unregister_hooks($class, \@hook_modules);
}

sub work_safely {
    my ($class, $manager, $job) = @_;
    my $res;

    if ($job->funcname->set_job_status) {
        $job->job_start_time = time;
    }
    eval {
        $res = $class->work($job);
    };

    if ( my $e = $@ || ! $job->is_completed ) {
        if ( $job->retry_cnt < $class->max_retries ) {
            $job->reenqueue(
                {
                    retry_cnt   => $job->retry_cnt + 1,
                    retry_delay => $class->retry_delay,
                }
            );
        } else {
            $manager->dequeue($job);
        }
        $manager->job_failed($job, $e || 'Job did not explicitly complete or fail');
    } else {
        $manager->dequeue($job);
    }

    return $res;
}

=head1 NAME

Qudo::Worker - superclass for defining task behavior of Qudo's work

=head1 SYNOPSIS

    package Myworker;
    use base qw/ Qudo::Worker /;

    sub work {
        my ($self , $job ) = @_;

        my $job_arg = $job->arg();
        print "This is Myworker's work. job has argument == $job_arg \n";

        $job->completed();
    }
    ### end of Myworker package.

=head1 DESCRIPTION

Qudo::Worker is based on all your work class of using Qudo.

Your application have to inherit Qudo::Worker anyway.
And it has to have 'work' method too.

'work' method accept Qudo::Job object at parameter.
If your work complete , you may call Qudo::Job->complete() method.

=head1 WORKER SETTING

=head2 max_retries

    package Your::Worker;
    use base 'Qudo::Worker';
    sub max_retries { 2 }
    sub work { ... }

How many times it retries if worker doesn't succeed is set.
It is retried two times in this example.
By default, return 0. no retry.

=head2 retry_delay

    package Your::Worker;
    use base 'Qudo::Worker';
    sub retry_delay { 10 }
    sub work { ... }

returns the number of seconds after a failure workers should wait until
retry a job that has already failed retry_delay times.
By default,return 0 seconds

=head2 grab_for

    package Your::Worker;
    use base 'Qudo::Worker';
    sub retry_delay { 60 }
    sub work { ... }

Returns the number of seconds workers of this class will claim a grabbed a job.
By default,return 3600 seconds.

=head2 set_job_status

    package Your::Worker;
    use base 'Qudo::Worker';
    sub set_job_status { 1 }
    sub work { ... }

set the flag.
When flag is the truth, the processing result of worker is preserved in DB. 

=cut

1;

