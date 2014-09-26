package Qudo::Test;
use strict;
use warnings;
use lib qw(./lib ./t/lib);
use Carp qw(croak);
use Qudo;
use YAML;
use DBI;
use Test::More;

our @SUPPORT_DRIVER = qw/Skinny/;

sub import {
    my $caller = caller(0);

    strict->import;
    warnings->import;

    for my $func (qw/run_tests run_tests_mysql run_tests_sqlite test_master teardown_dbs dsn_for/) {
        no strict 'refs'; ## no critic.
        *{$caller.'::'.$func} = \&$func;
    }
}

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

        for my $sql (@{ $ENV{USE_MYSQL} ? $schema->{mysql} : $schema->{SQLite} }) {
            $sql =~ s!^\s*create\s+table\s+(\w+)!CREATE TABLE $1!i;
            $sql .= " ENGINE=INNODB\n" if $ENV{USE_MYSQL};
            $dbh->do($sql);
        }

        $dbh->disconnect;
    }
}

my $schema_data;
sub load_schema {
    $schema_data ||= YAML::Load(join "", <DATA>);
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
    return(DBI->connect(
        "DBI:mysql:mysql",
        "root",
        "",
        { RaiseError => 1 }
    ) or die "Couldn't connect to database");
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
    my $dbname = shift;
    'test_qudo_' . $dbname . '.db';
}

sub mysql_dbname {
    my $dbname = shift;
    'test_qudo_' . $dbname;
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

__DATA__
SQLite:
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
        grabbed_until   INTEGER UNSIGNED NOT NULL,
        run_after       INTEGER UNSIGNED NOT NULL DEFAULT 0,
        retry_cnt       INTEGER UNSIGNED NOT NULL DEFAULT 0,
        priority        INTEGER UNSIGNED NOT NULL DEFAULT 0,
        UNIQUE(func_id,uniqkey)
    )
  - |-
    CREATE TABLE exception_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        func_id         INTEGER UNSIGNED NOT NULL DEFAULT 0,
        exception_time  INTEGER UNSIGNED NOT NULL,
        message         MEDIUMBLOB NOT NULL,
        uniqkey         VARCHAR(255) NULL,
        arg             MEDIUMBLOB,
        retried         TINYINT(1) NOT NULL DEFAULT 0
    );
  - |-
    CREATE TABLE job_status (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        func_id         INTEGER UNSIGNED NOT NULL DEFAULT 0,
        arg             MEDIUMBLOB,
        uniqkey         VARCHAR(255) NULL,
        status          VARCHAR(10),
        job_start_time  INTEGER UNSIGNED NOT NULL,
        job_end_time    INTEGER UNSIGNED NOT NULL
    );
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
        func_id         INTEGER UNSIGNED NOT NULL,
        arg             MEDIUMBLOB,
        uniqkey         VARCHAR(255) NULL,
        enqueue_time    INTEGER UNSIGNED,
        grabbed_until   INTEGER UNSIGNED NOT NULL,
        run_after       INTEGER UNSIGNED NOT NULL DEFAULT 0,
        retry_cnt       INTEGER UNSIGNED NOT NULL DEFAULT 0,
        priority        INTEGER UNSIGNED NOT NULL DEFAULT 0,
        UNIQUE(func_id, uniqkey),
        KEY priority (priority)
    )
  - |-
    CREATE TABLE exception_log (
        id              BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
        func_id         INTEGER UNSIGNED NOT NULL DEFAULT 0,
        exception_time  INTEGER UNSIGNED NOT NULL,
        message         MEDIUMBLOB NOT NULL,
        uniqkey         VARCHAR(255) NULL,
        arg             MEDIUMBLOB,
        retried         TINYINT(1) NOT NULL DEFAULT 0,
        INDEX (func_id),
        INDEX (exception_time)
    )
  - |-
    CREATE TABLE job_status (
        id              BIGINT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT,
        func_id         INTEGER UNSIGNED NOT NULL DEFAULT 0,
        arg             MEDIUMBLOB NOT NULL,
        uniqkey         VARCHAR(255) NULL,
        status          VARCHAR(10),
        job_start_time  INTEGER UNSIGNED NOT NULL,
        job_end_time    INTEGER UNSIGNED NOT NULL
    )
Pg:
  - |-
    CREATE TABLE func (
        id         SERIAL,
        name       VARCHAR(255) NOT NULL,
        PRIMARY KEY (id),
        UNIQUE (name)
    );
  - |-
    CREATE TABLE job (
        id              BIGSERIAL,
        func_id         INT NOT NULL,
        arg             BYTEA,
        uniqkey         VARCHAR(255) NULL,
        enqueue_time    INTEGER,
        grabbed_until   INTEGER  NOT NULL,
        run_after       INTEGER  NOT NULL DEFAULT 0,
        retry_cnt       INTEGER  NOT NULL DEFAULT 0,
        priority        INTEGER  NOT NULL DEFAULT 0,
        PRIMARY KEY (id),
        UNIQUE (func_id, uniqkey)
    );
  - |-
    CREATE TABLE exception_log (
        id              BIGSERIAL,
        func_id         INTEGER NOT NULL DEFAULT 0,
        exception_time  INTEGER NOT NULL,
        message         BYTEA,
        uniqkey         VARCHAR(255) NULL,
        arg             BYTEA,
        retried         SMALLINT,
        PRIMARY KEY (id)
    );
  - |-
    CREATE INDEX exception_log_func_id ON exception_log (func_id);
  - |-
    CREATE INDEX exception_log_exception_time ON exception_log (exception_time);
  - |-
    CREATE TABLE job_status (
        id              BIGSERIAL,
        func_id         INTEGER NOT NULL DEFAULT 0,
        arg             BYTEA,
        uniqkey         VARCHAR(255) NULL,
        status          VARCHAR(10),
        job_start_time  INTEGER NOT NULL,
        job_end_time    INTEGER NOT NULL,
        PRIMARY KEY (id)
    );
