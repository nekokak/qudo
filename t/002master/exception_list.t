use Qudo::Test;
use Test::More;

run_tests(8, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $job = $master->enqueue("Worker::Test1", {arg => 'arg', uniqkey => 'uniqkey'});
    $master->manager->job_failed($job, 'exception1');
    $master->manager->job_failed($job, 'exception2');

    my $list = $master->exception_list;
    my ($db, $rows) = each %$list;
    is scalar(@$rows), 2;

    $list = $master->exception_list({funcs => [qw/Worker::Test1/]});
    ($db, $rows) = each %$list;
    is scalar(@$rows), 2;

    $list = $master->exception_list({funcs => [qw/Worker::Test1/], limit => 1});
    ($db, $rows) = each %$list;
    is scalar(@$rows), 1;
    is $rows->[0]->{message}, 'exception1';

    $list = $master->exception_list({funcs => [qw/Worker::Test1/], limit => 1, offset => 1});
    ($db, $rows) = each %$list;
    is scalar(@$rows), 1;
    is $rows->[0]->{message}, 'exception2';

    $job = $master->enqueue("Worker::Test2", { arg => 'arg', uniqkey => 'uniqkey'});
    $master->manager->job_failed($job, 'exception3');

    $list = $master->exception_list;
    ($db, $rows) = each %$list;
    is scalar(@$rows), 3;

    $list = $master->exception_list({funcs => [qw/Worker::Test2/]});
    ($db, $rows) = each %$list;
    is scalar(@$rows), 1;

    teardown_dbs;
});

package Worker::Test1;
use base 'Qudo::Worker';

package Worker::Test2;
use base 'Qudo::Worker';
