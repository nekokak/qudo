use Qudo::Test;
use Test::More;

run_tests(4, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    $master->enqueue("Worker::Test1", {arg => 'arg1', uniqkey => 'uniqkey1'});
    $master->enqueue("Worker::Test2", {arg => 'arg2', uniqkey => 'uniqkey2'});

    my $row = $master->job_count;
    my ($dsn, $count) = each %$row; 
    is $count, 2;

    $row = $master->job_count([qw/Worker::Test1/]);
    ($dsn, $count) = each %$row;
    is $count, 1;

    $row = $master->job_count([qw/Worker::Test2/]);
    ($dsn, $count) = each %$row;
    is $count, 1;

    $row = $master->job_count([qw/Worker::Test1 Worker::Test2/]);
    ($dsn, $count) = each %$row;
    is $count, 2;

    teardown_dbs;
});

package Worker::Test1;
use base 'Qudo::Worker';

package Worker::Test2;
use base 'Qudo::Worker';
