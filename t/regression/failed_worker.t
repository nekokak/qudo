use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    $manager->work_once; # failed worker

    my $exception = $master->exception_list;
    my ($db, $rows) = %$exception;
    is $rows->[0]->{message}, 'Job did not explicitly complete or fail';

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
}
