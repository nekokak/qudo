use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->enqueue("Worker::Test", 'arg', 'uniqkey');
    $manager->enqueue("Worker::Test2", 'oops', 'uniqkey');
    is $manager->work_once, undef;

    teardown_db;
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    print STDOUT $job->arg;
    $job->completed;
}

package Worker::Test2;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    print STDOUT $job->arg;
    $job->completed;
}

