package Qudo::Test;
use strict;
use warnings;

use Exporter 'import';
our @EXPORT = qw/
    run_tests
     run_tests_mysql run_tests_sqlite
    test_client teardown_db
/;

use Carp qw(croak);
use Qudo;
use YAML;
use DBI;
use Test::More;

sub run_tests {
    my ($n, $code) = @_;

    run_tests_mysql($n, $code);
    run_tests_sqlite($n, $code);
}

sub run_tests_innodb {
    my ($n, $code) = @_;
    run_tests_mysql($n, $code, 1);
}

sub run_tests_mysql {
    my ($n, $code, $innodb) = @_;

    SKIP: {
        local $ENV{USE_MYSQL} = 1;
        my $dbh = eval { mysql_dbh() };
        skip "MySQL not accessible as root on localhost", $n if $@;
        skip "InnoDB not available on localhost's MySQL", $n if $innodb && ! has_innodb($dbh);
        $code->();
    }
}

sub run_tests_sqlite {
    my ($n, $code) = @_;

    SKIP: {
        my $rv = eval "use DBD::SQLite; 1";
        $rv = 0 if $ENV{SKIP_SQLITE};
        skip "SQLite not installed", $n if !$rv;
        $code->();
    }
}

sub test_client {
    my %opts = @_;
    my $dbname = delete $opts{dbname};
    my $init     = delete $opts{init};
    croak "unknown opts" if %opts;
    $init = 1 unless defined $init;

    if ($init) {
        setup_db($dbname);
    }

    return Qudo->new(
        database => +{
            dsn      => dsn_for($dbname),
            username => 'root',
            password => '',
        }
    );
}

sub setup_db {
    my $dbname = shift;

    my $schema = load_schema();
    teardown_db($dbname);

    if ($ENV{USE_MYSQL}) {
        create_mysql_db(mysql_dbname($dbname));
    }
    my $dbh = DBI->connect(
        dsn_for($dbname),
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

my $schema_data;
sub load_schema {
    $schema_data ||= YAML::Load(join "", <DATA>);
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
    return 't_qudo_' . $dbname . '.db';
}

sub mysql_dbname {
    my($dbname) = @_;
    return 't_qudo_' . $dbname;
}

sub create_mysql_db {
    my $dbname = shift;
    mysql_dbh()->do("CREATE DATABASE $dbname");
}

sub drop_mysql_db {
    my $dbname = shift;
    mysql_dbh()->do("DROP DATABASE IF EXISTS $dbname");
}

sub teardown_db {
    my $dbname = shift;
    if ($ENV{USE_MYSQL}) {
        drop_mysql_db(mysql_dbname($dbname));
    } else {
        my $file = db_filename($dbname);
        return unless -e $file;
        unlink $file or die "Can't teardown $dbname: $!";
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
__DATA__
sqlite:
  - |-
    CREATE TABLE func (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        name       VARCHAR(255) NOT NULL,
        UNIQUE(name)
    )
  - |-
    CREATE TABLE job (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        func_id         INTEGER UNSIGNED NOT NULL,
        arg             MEDIUMBLOB,
        uniqkey         VARCHAR(255) NULL,
        enqueue_time    INTEGER UNSIGNED,
        UNIQUE(func_id,uniqkey)
    )
  - |-
    CREATE TABLE exception_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        func_id         INTEGER UNSIGNED NOT NULL,
        job_id          INTEGER UNSIGNED NOT NULL,
        exception_time  INTEGER UNSIGNED NOT NULL,
        message         MEDIUMBLOB NOT NULL
    )

mysql:
  - |-
    CREATE TABLE func (
        id         INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
        name       VARCHAR(255) NOT NULL,
        UNIQUE(name)
    )
  - |-
    CREATE TABLE job (
        id              BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
        func_id         INT UNSIGNED NOT NULL,
        arg             MEDIUMBLOB,
        uniqkey         VARCHAR(255) NULL,
        enqueue_time    INTEGER UNSIGNED,
        UNIQUE(func_id, uniqkey)
    )
  - |-
    CREATE TABLE exception_log (
        id              BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
        func_id         INT UNSIGNED NOT NULL DEFAULT 0,
        job_id          BIGINT UNSIGNED NOT NULL,
        exception_time  INTEGER UNSIGNED NOT NULL,
        message         MEDIUMBLOB NOT NULL,
        INDEX (func_id),
        INDEX (exception_time),
        INDEX (job_id)
    )

