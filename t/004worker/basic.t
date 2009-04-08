use strict;
use warnings;
use Qudo::Test;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $master->enqueue("Worker::Test", 'arg', 'uniqkey');
    stdout_is( sub { $manager->work_once } , "arg");

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    print STDOUT $job->arg;
    $job->completed;
}

