package Qudo::Test;
use strict;
use warnings;

use Exporter 'import';
our @EXPORT = qw/
    run_tests
      run_tests_mysql run_tests_sqlite
    test_master
    teardown_dbs
/;

use Carp qw(croak);
use Qudo;
use YAML;
use DBI;
use Test::More;

our @SUPPORT_DRIVER = qw/Skinny DBI/;

sub run_tests {
    my ($n, $code) = @_;

    plan tests => $n*3*scalar(@SUPPORT_DRIVER);

    for my $driver (@SUPPORT_DRIVER) {
        run_tests_mysql( $n, $driver, $code);
        run_tests_innodb($n, $driver, $code);
        run_tests_sqlite($n, $driver, $code);
    }
}

sub run_tests_innodb {
    my ($n, $driver, $code) = @_;
    run_tests_mysql($n, $driver, $code, 1);
}

sub run_tests_mysql {
    my ($n, $driver, $code, $innodb) = @_;

    SKIP: {
        local $ENV{USE_MYSQL} = 1;
        my $dbh = eval { mysql_dbh() }; ## no critic
        skip "MySQL not accessible as root on localhost", $n if $@;
        skip "InnoDB not available on localhost's MySQL", $n if $innodb && ! has_innodb($dbh);
        $code->($driver);
    }
}

sub run_tests_sqlite {
    my ($n, $driver, $code) = @_;

    SKIP: {
        my $rv = eval "use DBD::SQLite; 1"; ## no critic
        $rv = 0 if $ENV{SKIP_SQLITE};
        skip "SQLite not installed", $n if !$rv;
        $code->($driver);
    }
}

my $test_dbs;
sub test_master {
    my %opts = @_;
    my $dbs  = delete $opts{dbs} || ['default'];
    my $init = delete $opts{init};
    $init = 1 unless defined $init;

    $test_dbs = $dbs;

    if ($init) {
        setup_dbs($dbs);
    }

    my $params = +{
        databases => [
            map {{
                dsn      => dsn_for($_),
                username => 'root',
                password => '',
            }} @$dbs
        ],
        %opts,
    };

    return Qudo->new(%$params);
}

sub setup_dbs {
    my $dbs = shift;

    my $schema = load_schema();
    teardown_dbs($dbs);

    for my $db (@$dbs) {
        if ($ENV{USE_MYSQL}) {
            create_mysql_db(mysql_dbname($db));
        }

        my $dbh = DBI->connect(
            dsn_for($db),
            'root',
            '',
            { RaiseError => 1, PrintError => 0 }
        ) or die "Couldn't connect: $!\n";

        for my $sql (@{ $ENV{USE_MYSQL} ? $schema->{mysql} : $schema->{sqlite} }) {
            $sql =~ s!^\s*create\s+table\s+(\w+)!CREATE TABLE $1!i;
            $sql .= " ENGINE=INNODB\n" if $ENV{USE_MYSQL};
            $dbh->do($sql);
        }

        $dbh->disconnect;
    }
}

my $schema_data;
sub load_schema {
    $schema_data->{mysql}  = load_sql('doc/schema-mysql.sql');
    $schema_data->{sqlite} = load_sql('doc/schema-sqlite.sql');
    $schema_data;
}

sub load_sql {
    my($file) = @_;
    open my $fh, '<', $file or die "Can't open $file: $!";
    my $sql = do { local $/; <$fh> };
    close $fh;
    my @sql = split /;\s*/, $sql;
    \@sql;
}

sub mysql_dbh {
    return DBI->connect(
        "DBI:mysql:mysql",
        "root",
        "",
        { RaiseError => 1 }
    ) or die "Couldn't connect to database";
}

sub dsn_for {
    my $dbname = shift;
    if ($ENV{USE_MYSQL}) {
        return 'dbi:mysql:' . mysql_dbname($dbname);
    } else {
        return 'dbi:SQLite:dbname=' . db_filename($dbname);
    }
}

sub db_filename {
    my($dbname) = @_;
    return 'test_qudo_' . $dbname . '.db';
}

sub mysql_dbname {
    my($dbname) = @_;
    return 'test_qudo_' . $dbname;
}

sub create_mysql_db {
    my $dbname = shift;
    mysql_dbh()->do("CREATE DATABASE $dbname");
}

sub drop_mysql_db {
    my $dbname = shift;
    mysql_dbh()->do("DROP DATABASE IF EXISTS $dbname");
}

sub teardown_dbs {
    my $dbs = shift || $test_dbs;

    for my $db (@$dbs) {
        if ($ENV{USE_MYSQL}) {
            drop_mysql_db(mysql_dbname($db));
        } else {
            my $file = db_filename($db);
            return unless -e $file;
            unlink $file or die "Can't teardown $db: $!";
        }
    }
}

sub has_innodb {
    my $dbh = shift;
    my $tmpname = "test_to_see_if_innoavail";
    $dbh->do("CREATE TABLE IF NOT EXISTS $tmpname (i int) ENGINE=INNODB")
        or return 0;
    my @row = $dbh->selectrow_array("SHOW CREATE TABLE $tmpname");
    my $row = join(' ', @row);
    my $has_it = ($row =~ /=InnoDB/i);
    $dbh->do("DROP TABLE $tmpname");
    return $has_it;
}

1;

