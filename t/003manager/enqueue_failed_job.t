use strict;
use warnings;
use Qudo::Test;
use Test::More;

run_tests(6, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    my $job_id = $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    my $job = $manager->lookup_job($job_id);

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    $manager->work_once; # worker failed.

    my $exception = $master->exception_list;
    $job_id = $manager->enqueue_failed_job($exception->[0]);
    $job = $manager->lookup_job($job_id);

    is $job->id, 2;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    teardown_db;
});

package Worker::Test;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    die 'failed';
}
