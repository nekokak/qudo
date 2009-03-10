use strict;
use warnings;
use Qudo::Test;
use Test::More;

run_tests(3, sub {
    my $driver = shift;
    my $master = test_master(
        dbname => 'tq1',
        driver => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    my $job = $manager->enqueue("Worker::Test", 'arg', 'uniqkey');

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';
