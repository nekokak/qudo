use Qudo::Test;
use Test::More;

run_tests(4, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my ($dsn, $cnt);

    ($dsn, $cnt) = each %{$master->job_count('Worker::Test2')};
    is $cnt, 0;
    ($dsn, $cnt) = each %{$master->job_count('Worker::Test3')};
    is $cnt, 0;

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $master->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    $manager->work_once;

    ($dsn, $cnt) = each %{$master->job_count('Worker::Test2')};
    is $cnt, 1;
    ($dsn, $cnt) = each %{$master->job_count('Worker::Test3')};
    is $cnt, 1;

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    $job->replace(
        [
            'Worker::Test2',
            {arg => 'arg'}
        ],
        [
            'Worker::Test3',
            {arg => 'arg'}
        ],
    );
}

