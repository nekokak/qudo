use Qudo::Test;
use Test::More;

run_tests(4, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');

    $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    my $job = $manager->find_job;

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    ok ! $manager->find_job;

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
sub grab_for    { 10 }

