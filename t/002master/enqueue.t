use Qudo::Test;
use Test::More;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $job = $master->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    is $job->id, 1;

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
