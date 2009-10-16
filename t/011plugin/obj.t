use strict;
use warnings;
use Qudo::Test;
use Test::Output;
use lib './t';

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->register_plugins(qw/Mock::Plugin::Object/);

    $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey1'});
    stdout_is( sub { $manager->work_once } , "arg");

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    $job->manager->plugin->{object}->stdout($job->arg);
}
