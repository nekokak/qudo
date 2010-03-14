use Qudo::Test;
use Test::Output;
use Test::Requires 'Test::Memory::Cycle';

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $master->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});

    memory_cycle_ok($master, 'master');

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    $job->completed;
}

