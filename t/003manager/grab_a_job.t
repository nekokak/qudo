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

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');

    $manager->enqueue("Worker::Test", 'arg', 'uniqkey');
    my $job = $manager->find_job;

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    ok ! $manager->find_job;

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';
sub grab_for    { 10 }

