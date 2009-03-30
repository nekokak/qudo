use strict;
use warnings;
use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        dbname       => 'tq1',
        driver_class => $driver,
        manager_abilities => [qw/Worker::Test/],
    );

    ok $master->manager->has_abilities;

    teardown_db('tq1');
});

