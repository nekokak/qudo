use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(7, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    {
        $manager->can_do('Worker::Test');
        $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list;
        is scalar(@$job_status), 0;
    }
    {
        $manager->can_do('Worker::Test2');
        my $job = $manager->enqueue("Worker::Test2", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list;
        is $job_status->[0]->{func_id}, $job->func_id;
        is $job_status->[0]->{status}, 'completed';
        is $job_status->[0]->{arg}, 'arg';
        is $job_status->[0]->{uniqkey}, 'uniqkey';
        is scalar(@$job_status), 1;
    }
    {
        $manager->can_do('Worker::Test3');
        my $job = $manager->enqueue("Worker::Test3", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list( funcs => ['Worker::Test2','Worker::Test3'] );
        is scalar(@$job_status), 2;

    }
    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub set_job_status { 0 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

package Worker::Test2;
use base 'Qudo::Worker';

sub set_job_status { 1 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

package Worker::Test3;
use base 'Qudo::Worker';

sub set_job_status { 1 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
}

