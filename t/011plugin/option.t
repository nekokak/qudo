use Qudo::Test;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->register_plugins(
        +{
            name => 'Mock::Plugin::Object2',
            option => +{
                foo => 'bar',
            },
        }
    );

    $manager->enqueue("Worker::Test", {});
    stdout_is( sub { $manager->work_once } , 'bar');

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    $job->manager->plugin->{object2}->call;
}
