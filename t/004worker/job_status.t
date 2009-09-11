use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

#BEGIN {
    @Qudo::Test::SUPPORT_DRIVER = qw/Skinny/;
#};

run_tests(3, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    {
        $manager->can_do('Worker::Test');
        my $job = $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list;
        is $job_status->[0]->{func_id}, $job->func_id;
        is $job_status->[0]->{status}, 'completed';
        is $job_status->[0]->{arg}, 'arg';
        is scalar(@$job_status), 1;

    }

    {
        $manager->can_do('Worker::Test2');
        $manager->enqueue("Worker::Test2", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list;
        is scalar(@$job_status), 1;
    }

    teardown_db;
});

package Worker::Test;
use base 'Qudo::Worker';

sub set_job_status { 1 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

package Worker::Test2;
use base 'Qudo::Worker';

sub set_job_status { 0 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

