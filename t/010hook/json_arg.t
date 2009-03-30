use strict;
use warnings;
use Qudo::Test;
use Test::More;
use lib './t';

run_tests(8, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');

    { # load Qudo::Hook::JSONArg
        $manager->register_hooks(qw/Qudo::Hook::JSONArg/);

        my $job_id = $manager->enqueue("Worker::Test", {key => 'arg'}, 'uniqkey1');
        my $job = $manager->lookup_job($job_id);

        is $job->id, 1;
        is $job->arg, '{"key":"arg"}';
        is $job->uniqkey, 'uniqkey1';

        my $res = $manager->work_once;
        is_deeply $res, {key => 'arg'};
    }

    { # unload Qudo::Hook::JSONArg
        $manager->unregister_hooks(qw/Qudo::Hook::JSONArg/);

        my $job_id = $master->manager->enqueue("Worker::Test", 'arg', 'uniqkey2');
        my $job = $manager->lookup_job($job_id);

        is $job->id, 2;
        is $job->arg, 'arg';
        is $job->uniqkey, 'uniqkey2';

        my $res = $manager->work_once;
        is $res, 'arg';
    }

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    $job->completed;
    return $job->arg;
}

