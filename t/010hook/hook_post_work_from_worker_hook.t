use strict;
use warnings;
use Qudo::Test;
use Test::Output;
use lib './t';

{
    package Worker::Test;
    use base 'Qudo::Worker';
    __PACKAGE__->register_hooks('Mock::Hook::PostWork');
    sub work {
        my ($self, $job) = @_;
        $job->completed();
    }
}

run_tests(2, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');

    $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey1'});
    stdout_is( sub { $manager->work_once } , "Worker::Test: post worked!\n");

    $manager->can_do('Worker::Test2');
    $manager->enqueue("Worker::Test2", { arg => 'arg', uniqkey => 'uniqkey1'});
    stdout_is( sub { $manager->work_once } , "arg\n");

    teardown_db;
});

package Worker::Test2;
use base 'Qudo::Worker';
sub work {
    my ($self, $job) = @_;
    print STDOUT $job->arg, "\n";
    $job->completed();
}
