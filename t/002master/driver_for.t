use Qudo::Test;
use Test::More;

run_tests(3, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
        dbs => [qw/db1 db2 db3/]
    );

    my @dbs = (
        'dbi:SQLite:dbname=test_qudo_db1.db',
        'dbi:SQLite:dbname=test_qudo_db2.db',
        'dbi:SQLite:dbname=test_qudo_db3.db',
    );
    for my $db (@dbs) {
        ok $master->driver_for($db);
    }

    teardown_dbs;
});
