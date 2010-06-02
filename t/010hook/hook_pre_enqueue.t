use Qudo::Test;
use Test::More;

run_tests(6, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->register_hooks(qw/Mock::Hook::Enqueue/);

    my $job = $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey1'});

    is $job->id, 1;
    is $job->arg, 'hooook';
    is $job->uniqkey, 'uniqkey1';

    $manager->unregister_hooks(qw/Mock::Hook::Enqueue/);

    $job = $master->manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey2'});

    is $job->id, 2;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey2';

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
