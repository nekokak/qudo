use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(3, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->enqueue("Worker::Test", { arg => 5, uniqkey => 'uniqkey'});

    stdout_is( sub {$manager->work_once}, 0 ); # fail job

    ok not $manager->find_job;

    sleep(5);

    stdout_is( sub {$manager->work_once}, 1 ); # check job

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub max_retries { 1 }
sub retry_delay { $_[1]->arg }
sub grab_for    { 0 }
sub work {
    my ($class, $job) = @_;

    print STDOUT $job->retry_cnt;
}
