use Qudo::Test;
use Test::More;

run_tests(4, sub {
    my $driver = shift;
    my @dbs = qw/db1 db2 db3/;
    my $master = test_master(
        driver_class => $driver,
        dbs => \@dbs,
    );

    my %expected = map {dsn_for($_) => 1} @dbs; 

    for my $db ($master->shuffled_databases) {
        ok delete $expected{$db};
    }

    is keys %expected, 0;

    teardown_dbs;
});
