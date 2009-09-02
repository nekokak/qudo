use strict;
use warnings;
use Qudo::Test;
use Test::More;
use lib './t';

run_tests(6, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->global_register_hooks(qw/Mock::Hook::Enqueue/);

    my $job_id = $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey1'});
    my $job = $manager->lookup_job($job_id);

    is $job->id, 1;
    is $job->arg, 'hooook';
    is $job->uniqkey, 'uniqkey1';

    $manager->global_unregister_hooks(qw/Mock::Hook::Enqueue/);

    $job_id = $master->manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey2'});
    $job = $manager->lookup_job($job_id);

    is $job->id, 2;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey2';

    teardown_db;
});

package Worker::Test;
use base 'Qudo::Worker';
