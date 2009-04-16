use strict;
use warnings;
use Qudo::Test;
use Test::More;
use lib './t';

run_tests(12, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->can_do('Worker::Test2');

    { # load Qudo::Hook::Serialize::JSON
        $manager->register_hooks(qw/Qudo::Hook::Serialize::JSON/);

        my $job_id = $manager->enqueue("Worker::Test", {key => 'arg'}, 'uniqkey1');
        my $job = $manager->lookup_job($job_id);

        is $job->id, 1;
        is $job->arg, '{"key":"arg"}';
        is $job->uniqkey, 'uniqkey1';

        sleep(1);

        my $res = $manager->work_once;
        is_deeply $res, {key => 'arg'};

    }

    { # failed worker by Qudo::Hook::Serialize::JSON
        my $job_id = $manager->enqueue("Worker::Test2", {key => 'arg'}, 'uniqkey1');
        my $job = $manager->lookup_job($job_id);

        is $job->id, 2;
        is $job->arg, '{"key":"arg"}';
        is $job->uniqkey, 'uniqkey1';

        sleep(1);

        $manager->work_once; # worker failed
        my $exception = $master->exception_list;
        is $exception->[0]->{arg}, '{"key":"arg"}';
    }

    { # unload Qudo::Hook::Serialize::JSON
        $manager->unregister_hooks(qw/Qudo::Hook::Serialize::JSON/);

        my $job_id = $master->manager->enqueue("Worker::Test", 'arg', 'uniqkey2');
        my $job = $manager->lookup_job($job_id);

        is $job->id, 3;
        is $job->arg, 'arg';
        is $job->uniqkey, 'uniqkey2';

        sleep(1);

        my $res = $manager->work_once;
        is $res, 'arg';
    }

    teardown_db;
});

package Worker::Test;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
    return $job->arg;
}

package Worker::Test2;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    my ($class, $job) = @_;
    die 'failed worker';
}
