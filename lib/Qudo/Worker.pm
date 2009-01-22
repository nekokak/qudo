package Qudo::Worker;
use strict;
use warnings;

sub max_retries { 0 }
sub retry_delay { 0 }

sub work_safely {
    my ($class, $manager, $job) = @_;
    my $res;

    eval {
        $res = $class->work($job);
    };
    if (my $e = $@) {
        $manager->job_failed($job, $e);
    }
    if (!$job->is_completed) {
        $manager->job_failed($job, 'Job did not explicitly complete, fail, or get replaced');
    }
}

1;
