use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(11, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    my $job = $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    $manager->work_once; # worker failed.

    my $exception = $master->exception_list;
    my ($db, $rows) = each %$exception;

    is $rows->[0]->{retried}, 0;
    $job = $manager->enqueue_from_failed_job($rows->[0], $db);

    is $job->id, 2;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';
    is $job->db, $db;

    $exception = $master->exception_list;
    ($db, $rows) = each %$exception;
    is $rows->[0]->{retried}, 1;

    stderr_like( sub {$manager->enqueue_from_failed_job($rows->[0], $db)}, qr/this exception is already retried/);

    is $master->job_count([qw/Worker::Test/]), 1;

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    die 'failed';
}
