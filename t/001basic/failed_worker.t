use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(3, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    my $job = $manager->enqueue("Worker::Test", 'arg', 'uniqkey');
    $manager->work_once;

    my $exception = $manager->driver->single('exception_log');
    like $exception->message, qr/failed worker/;
    is $exception->func_id, 1;
    is $exception->job_id, 1;

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    die "failed worker";
}
