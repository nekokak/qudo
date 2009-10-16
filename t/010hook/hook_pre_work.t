use Qudo::Test;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->global_register_hooks(qw/Mock::Hook::PreWork/);

    $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey1'});
    stdout_is( sub { $manager->work_once } , "Worker::Test: pre worked!\n");

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
