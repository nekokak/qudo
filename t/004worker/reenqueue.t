use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

@Qudo::Test::SUPPORT_DRIVER = qw/Skinny/;

run_tests(2, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    my $job_id = $manager->enqueue("Worker::Test", 'arg', 'uniqkey');

    stdout_is( sub {$manager->work_once}, 0 ); # fail job

    sleep(1);

    stdout_is( sub {$manager->work_once}, 1 ); # check job

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';

sub max_retries { 1 }
sub work {
    my ($class, $job) = @_;

    print STDOUT $job->retry_cnt;
}
