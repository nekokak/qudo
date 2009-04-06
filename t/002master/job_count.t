use strict;
use warnings;
use Qudo::Test;
use Test::More;

run_tests(4, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
    );

    $master->enqueue("Worker::Test1", 'arg1', 'uniqkey1');
    $master->enqueue("Worker::Test2", 'arg2', 'uniqkey2');

    is $master->job_count, 2;
    is $master->job_count([qw/Worker::Test1/]), 1;
    is $master->job_count([qw/Worker::Test2/]), 1;
    is $master->job_count([qw/Worker::Test1 Worker::Test2/]), 2;

    teardown_db('tq1');
});

package Worker::Test1;
use base 'Qudo::Worker';

package Worker::Test2;
use base 'Qudo::Worker';
