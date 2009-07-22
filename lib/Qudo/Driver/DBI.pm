package Qudo::Driver::DBI;

use strict;
use warnings;
use DBI;
use Carp qw/croak/;

use Qudo::Driver::DBI::DBD;

use Data::Dumper;

sub init_driver {
    my ($class, $master) = @_;

    my $self = bless {
        database => $master->{database} ,
        dbh      => '',
        dbd      => '',
    }, $class;
    $self->_connect();

    my $dbd_type = $self->{dbh}->{Driver}->{Name};
    $self->{dbd} = Qudo::Driver::DBI::DBD->new($dbd_type) or die;
    
    return $self;
}

sub _connect {
    my $class = shift;
    
    $class->{dbh} =  DBI->connect(
        $class->{database}->{dsn},
        $class->{database}->{username},
        $class->{database}->{password},
        { RaiseError => 1, PrintError => 0, AutoCommit => 1, %{ $class->{database}->{connect_options} || {} } }
    );
    
    return $class;
}

sub job_count{
    my ($class , $funcs) = @_;

    my $sql = q{
        SELECT
            COUNT(job.id) AS count
        FROM
            job, func
        WHERE
            job.func_id = func.id };
    if( $funcs ){
        $sql .= sprintf( q{ AND func.name IN (%s) },join(',', map { '?' } @{$funcs} ) );
    }

    my $sth = $class->{dbh}->prepare( $sql );

    eval{
        $sth->execute( @{$funcs} );
    };
    if( my $e =  $@ ){
        croak 'job_count ERROR'.$e;
    }
    my $ret = $sth->fetchrow_hashref();
    return $ret->{count};
}

sub job_list{
    my ($class, $limit, $funcs) = @_;

    my $sql = $class->_search_job_sql();
    my @bind = $class->get_server_time;

    # func.name
    if( $funcs ){
        $sql .= sprintf( q{
            AND
                (func.name IN (%s))
            },join(',', map { '?' } @{$funcs} )
        );
        push @bind , @{$funcs};
    }

    # limit
    $sql .= q{LIMIT ?};
    push @bind , $limit;


    my $sth = $class->{dbh}->prepare( $sql );

    eval{
        $sth->execute( @bind );
    };
    if( my $e =  $@ ){
        croak 'job_list ERROR'.$e;
    }

    my $code =  $class->_get_job_data( $sth );

    my @jobs;
    while (1) {
        my $row = $code->();
        last unless $row;
        push @jobs, $row;
    }
    return \@jobs;
}


sub exception_list{
    my ($class, %args) = @_;

    my @bind   = ();
    my $limit  = $args{limit};
    my $offset = $args{offset};
    my $funcs  = $args{funcs} || '';
    my $sql = q{
        SELECT
            exception_log.id,
            exception_log.func_id,
            exception_log.exception_time,
            exception_log.message,
            exception_log.uniqkey,
            exception_log.arg
        FROM
            exception_log
    };

    # funcs
    if ($funcs) {
        $sql .= sprintf(
            q{
                INNER JOIN
                    func
                ON
                    exception_log.func_id = func.id
                WHERE
                    ( func.name IN(%s) )
            },join(',', map { '?' } @{$funcs} )
        );
        push @bind , @{$funcs};
    }

    # limit
    if( $limit ){
        $sql .= q{ LIMIT ? };
        push @bind , $limit;
    }

    #offset
    if( $offset ){
        $sql .= q{OFFSET ?};
        push @bind , $offset;
    }

    my $sth = $class->{dbh}->prepare( $sql );
    eval{
        $sth->execute( @bind );
    };
    if( my $e =  $@ ){
        croak 'exception_list ERROR'.$e;
    }
    my @exception_list;
    while (my $row = $sth->fetchrow_hashref) {
        push @exception_list, $row;
    }
    return \@exception_list;
}


sub lookup_job {
    my ($class, $job_id) = @_;

    my $sql = $class->_search_job_sql();
    my @bind = $class->get_server_time;

    # func.name
    if( $job_id ){
        $sql .= q{ AND (job.id = ?)};
        push @bind , $job_id;
    }

    # limit
    $sql .= q{LIMIT ?};
    push @bind , 1;

    my $sth = $class->{dbh}->prepare( $sql );

    eval{
        $sth->execute( @bind );
    };
    if( my $e =  $@ ){
        croak 'lookup_job ERROR'.$e;
    }

    return $class->_get_job_data( $sth );
}

sub find_job {
    my ($class, $limit, $func_map) = @_;

    my $sql = $class->_search_job_sql();
    my @bind = $class->get_server_time;

    # func.name
    if( $func_map ){
        my $keys = [keys %$func_map];
        $sql .= sprintf( q{
            AND
                (func.name IN (%s))
            },join(',', map { '?' } @{$keys} )
        );
        push @bind , @{$keys};
    }

    # limit
    $sql .= q{LIMIT ?};
    push @bind , $limit;

    my $sth = $class->{dbh}->prepare( $sql );

    eval{
        $sth->execute( @bind );
    };
    if( my $e =  $@ ){
        croak 'find_job ERROR'.$e;
    }

    return $class->_get_job_data( $sth );
}

sub _search_job_sql{
    my $class = shift;

    my $sql = q{
        SELECT
            job.id AS id,
            job.arg AS arg,
            job.uniqkey AS uniqkey,
            job.func_id AS func_id,
            job.grabbed_until AS grabbed_until,
            job.retry_cnt AS retry_cnt,
            func.name AS funcname
        FROM job
        INNER JOIN
            func ON job.func_id = func.id
        WHERE
            (job.grabbed_until <= ?)
    };
    return $sql;
}


sub _get_job_data {
    my ($class, $sth) = @_;
    sub{
        while (my $row = $sth->fetchrow_hashref) {
            return +{
                job_id            => $row->{id},
                job_arg           => $row->{arg},
                job_uniqkey       => $row->{uniqkey},
                job_grabbed_until => $row->{grabbed_until},
                job_retry_cnt     => $row->{retry_cnt},
                func_id           => $row->{func_id},
                func_name         => $row->{funcname},
            };
        }
    };
}

sub grab_a_job {
    my ($class, %args) = @_;

    my $sth = $class->{dbh}->prepare(
        q{
        UPDATE
            job
        SET
            grabbed_until = ?
        WHERE
            id = ?
        AND
            grabbed_until = ? }
    );

    my $rows;
    eval{
        $rows = $sth->execute(
            $args{grabbed_until},
            $args{job_id},
            $args{old_grabbed_until},
        );
    };
    if( my $e =  $@ ){
        croak 'grab_a_job ERROR'.$e;
        return;
    }

    return $rows;
}

sub logging_exception {
    my ($class, $args) = @_;

    my $sth = $class->{dbh}->prepare(
        q{ INSERT INTO
            exception_log  ( func_id , message , uniqkey, arg, exception_time ) }
        . q{ VALUES ( ? , ? , ?, ?, ?) }
    );

    eval{
        $sth->execute(
            $args->{func_id} , 
            $args->{message} , 
            $args->{uniqkey} , 
            $args->{arg} , 
            time(),
        );
    };
    if( my $e =  $@ ){
        croak 'logging_exception ERROR'.$e;
    }
    return;
}

sub get_server_time {
    my $class = shift;

    my $unixtime_sql = $class->{dbd}->sql_for_unixtime;
    my $time;
    eval {
        $time = $class->{dbh}->selectrow_array("SELECT $unixtime_sql");
    };
    if ($@) { $time = time }
    return $time;
}

sub enqueue {
    my ($class, $args) = @_;

    $args->{enqueue_time}  ||= time;
    $args->{grabbed_until} ||= 0;
    $args->{retry_cnt}     ||= 0;

    my @column = keys %{$args};
    my $sql  = 'INSERT INTO job ( ';
       $sql .= join ' ,' , @column;
       $sql .= ' ) VALUES ( ';
       $sql .= join(', ', ('?') x @column);
       $sql .=  ')';

    my $sth_ins = $class->{dbh}->prepare( $sql );
    my @bind = map {$args->{$_}} @column;
    eval{
        $sth_ins->execute( @bind );
    };
    if( $@ ){
        croak 'enqueue ERROR'.$@;
    }

    my $id = $class->{dbd}->last_insert_id($class->{dbh}, $sth_ins);
    my $sth_sel = $class->{dbh}->prepare(
        q{SELECT * FROM job WHERE id = ?}
    );

    $sth_sel->execute( $id );
    my $ret_sel = $sth_sel->fetchrow_hashref();
    return $ret_sel ? $ret_sel->{id} : undef;
}

sub reenqueue {
    my ($class, $job_id, $args) = @_;

    my $sth = $class->{dbh}->prepare(
        q{
        UPDATE
            job
        SET
            enqueue_time = ?,
            retry_cnt = ?
        WHERE
            id = ? }
    );

    my $row;
    eval{
        $row = $sth->execute(
            (time + ($args->{retry_delay}||0) ),
            $args->{retry_cnt},
            $job_id,
        );
    };
    if( my $e =  $@ ){
        croak 'reenqueue ERROR'.$e;
        return;
    }

    return $row;
}


sub dequeue {
    my ($class, $args) = @_;
    my $sth = $class->{dbh}->prepare(
        q{ DELETE FROM  job WHERE id = ? }
    );

    my $row;
    eval{
        $row = $sth->execute( $args->{id} );
    };
    if( my $e = $@ ){
        croak 'dequeue ERROR'.$e;
    }

    return $row;
}


sub get_func_id {
    my ($class, $funcname) = @_;
    
    my $sth_sel = $class->{dbh}->prepare(
        q{SELECT * FROM func WHERE name = ?}
    );

    $sth_sel->execute( $funcname );
    my $func_id;
    my $ret_hashref = $sth_sel->fetchrow_hashref();
    if ( $ret_hashref ){
        $func_id =  $ret_hashref->{id};
    }
    else{
        my $sth_ins = $class->{dbh}->prepare(
            q{INSERT INTO func ( name ) VALUES ( ? )}
        );
        eval{
            $sth_ins->execute( $funcname );
        };
        if( my $e = $@ ){
            croak $e;
        }
        $sth_sel->execute( $funcname );
        my $ret_hashref = $sth_sel->fetchrow_hashref();
        if ( $ret_hashref ){
            $func_id =  $ret_hashref->{id};
        }
    }

    return $func_id;
}

1;

