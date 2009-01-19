package Qudo::Worker;
use strict;
use warnings;

sub max_retries { 0 }
sub retry_delay { 0 }

sub work_safely {
    my ($class, $client, $job) = @_;
    my $res;

    eval {
        $res = $class->work($job);
    };

    if ($@) {
        $client->failed($@);
    }
    unless ($job->complete) {
        $client->failed('Job did not explicitly complete, fail, or get replaced');
    }
}

1;
