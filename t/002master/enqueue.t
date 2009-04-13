use strict;
use warnings;
use Qudo::Test;
use Test::More;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $job_id = $master->enqueue("Worker::Test", 'arg', 'uniqkey');
    is $job_id, 1;

    teardown_db;
});

package Worker::Test;
use base 'Qudo::Worker';
