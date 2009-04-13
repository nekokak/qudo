use strict;
use warnings;
use Qudo::Test;
use Test::More;

run_tests(8, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $job_id = $master->enqueue("Worker::Test1", 'arg', 'uniqkey');
    my $job = $master->manager->lookup_job($job_id);
    $master->manager->job_failed($job, 'exception1');
    $master->manager->job_failed($job, 'exception2');

    my $list = $master->exception_list;
    is scalar(@$list), 2;

    $list = $master->exception_list(funcs => [qw/Worker::Test1/]);
    is scalar(@$list), 2;

    $list = $master->exception_list(funcs => [qw/Worker::Test1/], limit => 1);
    is scalar(@$list), 1;
    is $list->[0]->{message}, 'exception1';

    $list = $master->exception_list(funcs => [qw/Worker::Test1/], limit => 1, offset => 1);
    is scalar(@$list), 1;
    is $list->[0]->{message}, 'exception2';

    $job_id = $master->enqueue("Worker::Test2", 'arg', 'uniqkey');
    $job = $master->manager->lookup_job($job_id);
    $master->manager->job_failed($job, 'exception3');

    $list = $master->exception_list;
    is scalar(@$list), 3;

    $list = $master->exception_list(funcs => [qw/Worker::Test2/]);
    is scalar(@$list), 1;

    teardown_db;
});

package Worker::Test1;
use base 'Qudo::Worker';

package Worker::Test2;
use base 'Qudo::Worker';
