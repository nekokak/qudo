use Qudo::Test;
use Test::More;

run_tests(3, sub {
    my $driver = shift;
    my @dbs = qw/db1 db2 db3/;
    my $master = test_master(
        driver_class => $driver,
        dbs => \@dbs,
    );

    for my $db (@dbs) {
        ok $master->driver_for(dsn_for($db));
    }

    teardown_dbs;
});
