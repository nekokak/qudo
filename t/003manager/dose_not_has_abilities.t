use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(1, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    ok ! $master->manager->has_abilities;

    teardown_dbs;
});

