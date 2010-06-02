use Qudo::Test;
use Test::More;

run_tests(2, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    $master->enqueue("Worker::Test1", { arg => 'arg1', uniqkey => 'uniqkey1'});
    $master->enqueue("Worker::Test2", { arg => 'arg2', uniqkey => 'uniqkey2'});
    my $lists = $master->job_list;

    my @result = map { +{job_arg => $_->{job_arg} } } @$lists;
    is_deeply \@result, [
        +{job_arg => 'arg1'},
        +{job_arg => 'arg2'},
    ];

    $lists = $master->job_list([qw/Worker::Test1/]);
    @result = map { +{ job_arg => $_->{job_arg} } } @$lists;
    is_deeply \@result, [
        +{job_arg => 'arg1'},
    ];

    teardown_dbs;
});

package Worker::Test1;
use base 'Qudo::Worker';

package Worker::Test2;
use base 'Qudo::Worker';
