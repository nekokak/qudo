package Qudo::Worker;
use strict;
use warnings;

sub max_retries { 0 }
sub retry_delay { 0 }
sub grab_for    { 0 }

sub work_safely {
    my ($class, $manager, $job) = @_;
    my $res;

    eval {
        $res = $class->work($job);
    };
    if ($@) {
        $manager->job_failed($job, $@);
    }
    if ( $job->is_completed ) {
        $manager->dequeue($job);
    } else {
        if ( $job->retry_cnt < $class->max_retries ) {
            $job->reenqueue(
                {
                    retry_cnt   => $job->retry_cnt + 1,
                    retry_delay => $class->retry_delay,
                }
            );
        }
        $manager->job_failed($job, 'Job did not explicitly complete, fail, or get replaced');
    }

    return $res;
}

1;
