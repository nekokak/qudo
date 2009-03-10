use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

@Qudo::Test::SUPPORT_DRIVER = qw/Skinny/;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        dbname   => 'tq1',
        driver   => $driver,
    );

    my $manager = $master->manager;
    $manager->enqueue("Worker::Test", 'arg', 'uniqkey');
    $manager->enqueue("Worker::Test2", 'oops', 'uniqkey');
    is $manager->work_once, undef;

    teardown_db('tq1');
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

