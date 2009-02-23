package Qudo::Driver::DBI;

use strict;
use warnings;
use DBI;
use Carp qw/croak/;

use Data::Dumper;

sub init_driver {
    my ($class, $master) = @_;

    my $self = bless {
        database => $master->{database} ,
        dbh      => '',
    }, $class;
    $self->_connect();

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

sub lookup_job {
    my ($class, $job_id) = @_;

    my $sth = $class->{dbh}->prepare(
        q{
        SELECT
            job.id AS id,
            job.arg AS arg,
            job.uniqkey AS uniqkey,
            job.func_id AS func_id,
            job.grabbed_until,
            func.name AS funcname
        FROM
            job, func
        WHERE
            job.func_id = func.id AND
            job.id      = ?
        LIMIT 1 }
    );

    eval{
        $sth->execute( $job_id );
    };
    if( my $e =  $@ ){
        croak 'lookup_job ERROR'.$e;
    }

    return $class->_get_job_data( $sth->fetchrow_hashref() );
}

sub find_job {
    my $class = shift;

    my $sth = $class->{dbh}->prepare(
        q{
        SELECT
            job.id AS id,
            job.arg AS arg,
            job.uniqkey AS uniqkey,
            job.func_id AS func_id,
            job.grabbed_until,
            func.name AS funcname
        FROM
            job, func
        WHERE
            job.func_id = func.id
        LIMIT 10 }
    );

    eval{
        $sth->execute( );
    };
    if( my $e =  $@ ){
        croak 'find_job ERROR'.$e;
    }

    return $class->_get_job_data($sth->fetchrow_hashref);
}

sub _get_job_data {
    my ($class, $hash_ref) = @_;
    sub{
        return +{
            job_id            => $hash_ref->{id},
            job_arg           => $hash_ref->{arg},
            job_uniqkey       => $hash_ref->{uniqkey},
            job_grabbed_until => $hash_ref->{grabbed_until},
            func_id           => $hash_ref->{func_id},
            func_name         => $hash_ref->{funcname},
        };
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

    eval{
        $sth->execute(
            $args{grabbed_until},
            $args{job_id},
            $args{old_grabbed_until},
        );
    };
    if( my $e =  $@ ){
        croak 'grab_a_job ERROR'.$e;
        return;
    }

    return 1;
}

sub logging_exception {
    my ($class, $args) = @_;

    my $sth = $class->{dbh}->prepare(
        q{ INSERT INTO
            exception_log  ( job_id , func_id , message , exception_time ) }
        . q{ VALUES ( ? , ? , ? , ?) }
    );

    eval{
        $sth->execute(
            $args->{job_id} , 
            $args->{func_id} , 
            $args->{message} , 
            time(),
        );
    };
    if( my $e =  $@ ){
        croak 'logging_exception ERROR'.$e;
        return;
    }

    return 1;
}

sub get_server_time {
    my $class = shift;
#    my $unixtime_sql = $class->dbd->sql_for_unixtime;
    my $unixtime_sql =  "UNIX_TIMESTAMP()";
# SQLIite
#    my $unixtime_sql =  time();
    my $time;
    eval {
        $time = $class->{dbh}->selectrow_array("SELECT $unixtime_sql");
    };
    if ($@) { $time = time }
    return $time;
}

sub enqueue {
    my ($class, $args) = @_;

    $args->{enqueue_time} = time;
    $args->{grabbed_until} ||= 0;

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

    my $id = $class->last_insert_id($sth_ins);
    my $sth_sel = $class->{dbh}->prepare(
        q{SELECT * FROM job WHERE id = ?}
    );

    $sth_sel->execute( $id );
    my $ret_sel = $sth_sel->fetchrow_hashref();
    return $ret_sel ? $ret_sel->{id} : undef;
}

sub last_insert_id{
    my ($class, $sth) = @_;

    # FIXME: tekitou
    #mysql                                                      # sqlite
    my $last_id = $sth->{mysql_insertid} || $sth->{insertid} || $class->{dbh}->func('last_insert_rowid');

    return $last_id;
}

sub dequeue {
    my ($class, $args) = @_;
    my $sth = $class->{dbh}->prepare(
        q{ DELETE FROM  job WHERE id = ? }
    );

    eval{
        $sth->execute( $args->{id} );
    };
    if( my $e = $@ ){
        croak 'dequeue ERROR'.$e;
    }

    return ;
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


sub single{
    my ($class , $table , $where , $opt) = @_;

    $opt->{limit} = 1;

    my $q_where;
    my @exe_ary;
    if ( $where ){
        $q_where = join( "and" , map { "$_ = ?" } sort keys %{$where} );
        map { push @exe_ary , $where->{$_} } sort keys %{$where};
    }
    my $q_opt =  join( "and" , map { "$_  $opt->{$_}" } keys %{$opt} );
    map { push @exe_ary , $opt->{$_} } sort keys %{$opt};
        
    my $sth = $class->{dbh}->prepare( qq{
        SELECT * FROM $table } .
        ($q_where ? qq{ WHERE $q_where } : q{} ).
        qq{ $q_opt } );

    $sth->execute();
    
    my $ret_hashref = $sth->fetchrow_hashref();
    if ( $ret_hashref ){
        my $ret_class = bless $ret_hashref , 'Ret::Class';
        my @ary;
        push @ary , keys %{$ret_class};
        'Ret::Class'->mk_accessors ( @ary );

        return $ret_class;
    }

    return ;
}

package Ret::Class;
use base qw/ Class::Accessor::Fast /;

1;

