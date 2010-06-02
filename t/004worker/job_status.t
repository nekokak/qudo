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
        $manager->work_once; # not logging job_status

        my $job_status = $master->job_status_list;
        my ($dsn, $rows)  = each %$job_status;
        is scalar(@$rows), 0;
    }
    {
        $manager->can_do('Worker::Test2');
        my $job = $manager->enqueue("Worker::Test2", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list;
        my ($dsn, $rows)  = each %$job_status;
        is $rows->[0]->{func_id}, $job->func_id;
        is $rows->[0]->{status}, 'completed';
        is $rows->[0]->{arg}, 'arg';
        is $rows->[0]->{uniqkey}, 'uniqkey';
        is scalar(@$rows), 1;
    }
    {
        $manager->can_do('Worker::Test3');
        my $job = $manager->enqueue("Worker::Test3", { arg => 'arg', uniqkey => 'uniqkey'});
        $manager->work_once; # logging job_status

        my $job_status = $master->job_status_list({ funcs => ['Worker::Test2','Worker::Test3'] });
        my ($dsn, $rows)  = each %$job_status;
        is scalar(@$rows), 2;

    }
    {
        $manager->can_do('Worker::Test4');
        $manager->enqueue("Worker::Test4", { arg => 'arg', uniqkey => 'uniqkey'});
        my $job = $manager->work_once; # logging failed job_status

        my $job_status = $master->job_status_list({ funcs => ['Worker::Test2','Worker::Test3','Worker::Test4'] });
        my ($dsn, $rows)  = each %$job_status;
        is scalar(@$rows), 3;

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

package Worker::Test4;
use base 'Qudo::Worker';

sub set_job_status { 1 }
sub work {
    my ($class, $job) = @_;
    die 'ooops worker::test4 is failed';
}

