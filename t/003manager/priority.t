use Qudo::Test;
use Test::More;
use List::Util;

my $expected_priority;
run_tests(10, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');

    my @job_priority = (1..10);

    for my $priority (@job_priority) {
        $manager->enqueue(
            "Worker::Test",
            {
                arg      => 'arg'.$priority,
                priority => $priority,
            }
        );
    }

    for (reverse @job_priority) {
        $expected_priority = $_;
        $manager->work_once;
    }

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
use Test::More;

sub work {
    my ($class, $job,) = @_;
    is $job->priority, $expected_priority;
    $job->completed;
}

