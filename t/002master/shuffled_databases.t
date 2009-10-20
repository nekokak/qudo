use Qudo::Test;
use Test::More;

run_tests(4, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
        dbs => [qw/db1 db2 db3/]
    );

    my @dbs = $master->shuffled_databases;
    my %expected = (
        'dbi:SQLite:dbname=test_qudo_db1.db' => 1,
        'dbi:SQLite:dbname=test_qudo_db2.db' => 1,
        'dbi:SQLite:dbname=test_qudo_db3.db' => 1,
    );

    for my $db (@dbs) {
        ok delete $expected{$db};
    }

    is keys %expected, 0;

    teardown_dbs;
});
