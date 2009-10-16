use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(2, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->enqueue("Worker::Test", { run_after => 5, arg => 'do job', uniqkey => 'uniqkey' });

    ok not $manager->find_job;

    sleep(5);
    
    stdout_is( sub {$manager->work_once}, 'do job' );

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
sub work {
    my ($class, $job) = @_;

    print STDOUT $job->arg;
}
