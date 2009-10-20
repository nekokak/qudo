use Qudo::Test;
use Test::More;

run_tests(2, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $db = $master->shuffled_databases;
    my $manager = $master->manager;

    my $func_id = $manager->funcname_to_id( 'Worker::Test', $db );
    is $func_id, 1;

    my $func_name = $manager->funcid_to_name($func_id, $db);
    is $func_name, 'Worker::Test';

    teardown_dbs;
});

