use strict;
use warnings;
use Qudo::Test;
use Test::Output;
use Test::More;

run_tests(3, sub {
    my $master = test_master(
        dbname   => 'tq1',
        driver   => 'DBI',
    );

    my $manager = $master->manager;
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
