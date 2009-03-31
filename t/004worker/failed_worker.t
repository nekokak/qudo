use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

@Qudo::Test::SUPPORT_DRIVER = qw/Skinny/;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->enqueue("Worker::Test", 'arg', 'uniqkey');
    $manager->work_once;

    my $exception = $master->exception_list;
    like $exception->[0]->{message}, qr/failed worker/;

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    die "failed worker";
}
