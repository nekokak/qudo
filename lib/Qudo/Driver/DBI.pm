package Qudo::Driver::DBI;

use DBI;
use Carp qw/croak/;

use Data::Dumper;


sub init_driver {
    my ($class, $qudo) = @_;

    $class->reconnect($qudo->{database});

    return $class;
}

=test
sub connect_info {
    my ($class, $connect_info) = @_;

    $class->attribute->{dsn} = $connect_info->{dsn};
    $class->attribute->{username} = $connect_info->{username};
    $class->attribute->{password} = $connect_info->{password};
    $class->attribute->{connect_options} = $connect_info->{connect_options};

    my $dbd_type = _dbd_type($connect_info);
    $class->attribute->{dbd} = DBIx::Skinny::DBD->new($dbd_type);
}
=cut

my $DBH;
sub dbh{
    my $self = shift;
    
    if( @_ ){
        $DBH = shift @_;
    }
    return $DBH;
}

sub _connect {
    my ($class , $qudo )  = @_;
    
    $DBH = undef  if $qudo->{flush};
    if( ! $class->dbh ){
        my $dbh = DBI->connect(
            $qudo->{dsn},
            $qudo->{username},
            $qudo->{password},
            { RaiseError => 1, PrintError => 0, AutoCommit => 1, %{ $qudo->{connect_options} || {} } }
        );

        $class->dbh( $dbh );
    }
    return $class->dbh;
}

sub reconnect {
    my $class    = shift;
    my $database = shift;
    
    $database->{flush} = 1;
#    $class->connect_info(@_);
    $class->_connect( $database );
}

sub lookup_job {
    my ($class, $job_id) = @_;

    my $sel = $class->dbh->prepare( q/
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
        LIMIT 1
    /);

    eval{
        $sel->execute( $job_id );
    };
    if( my $e =  $@ ){
        croak 'lookup_job ERROR'.$e;
    }

    return $class->_get_job_data( $sel->fetchrow_hashref() );
}

sub find_job {
    my $class = shift;

    my $sel = $class->dbh->prepare( q/
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
        LIMIT 10
    /);

    eval{
        $sel->execute( );
    };
    if( my $e =  $@ ){
        croak 'find_job ERROR'.$e;
    }

    return $class->_get_job_data($sel->fetchrow_hashref);
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

    my $upd = $class->dbh->prepare( q/
        UPDATE
            job
        SET
            grabbed_until = ?
        WHERE
            id = ?
        AND
            grabbed_until = ?
    /);

    eval{
        $upd->execute(
            $args{grabbed_until},
            $args{job_id},
            $args{old_grabbed_until},
        );
    };
    if( my $e =  $@ ){
        croak 'grab_a_job ERROR'.$e;
    }

    return $upd->rows();
}

sub logging_exception {
    my ($class, $args) = @_;

    my $ins = $class->dbh->prepare( q/
        INSERT INTO exception_log  ( job_id , func_id , message , exception_time ) / 
        . q/ VALUES ( ? , ? , ? , ?)/ 
    );

    eval{
        $ins->execute( 
            $args->{job_id} , 
            $args->{func_id} , 
            $args->{message} , 
            time(),
        );
    };
    if( my $e =  $@ ){
        croak 'logging_exception ERROR'.$e;
    }

    return $ins->rows();
}

sub get_server_time {
    my $class = shift;
#    my $unixtime_sql = $class->dbd->sql_for_unixtime;
    my $unixtime_sql =  "UNIX_TIMESTAMP()";
# SQLIite
#    my $unixtime_sql =  time();
    my $time;
    eval {
        $time = $class->dbh->selectrow_array("SELECT $unixtime_sql");
    };
    if ($@) { $time = time }
    return $time;
}

sub enqueue {
    my ($class, $args) = @_;

    $args->{enqueue_time} = time;
    $args->{grabbed_until} ||= 0;

    my @colum = sort keys %{$args};
    my $sql  = 'INSERT INTO job ( ';
       $sql .= join ' ,' , @colum;
       $sql .= ' ) VALUES ( ';
       $sql .= join(', ', ('?') x @colum);
       $sql .=  ')';

    my $ins = $class->dbh->prepare( $sql );
    my @bind = map {$args->{$_}} @colum;
    eval{
        $ins->execute( @bind );
    };
    if( $@ ){
        croak 'enqueue ERROR'.$@;
    }

    my $id = $class->last_insert_id($ins);
    my $sel = $class->dbh->prepare(
        q{SELECT * FROM job WHERE id = ?}
    );

    $sel->execute( $id );
    my $sel_ret = $sel->fetchrow_hashref();
    return $sel_ret ? $sel_ret->{id} : undef;
}

sub last_insert_id{
    my ($class, $sth) = @_;

    # FIXME: tekitou
    #mysql                                                      # sqlite
    my $last_id = $sth->{mysql_insertid} || $sth->{insertid} || $class->dbh->func('last_insert_rowid');

    return $last_id;
}

sub dequeue {
    my ($class, $args) = @_;
    my $del = $class->dbh->prepare( q/
        DELETE FROM  job WHERE id = ? /
    );

    eval{
        $del->execute( $args->{id} );
    };
    if( my $e = $@ ){
        croak 'dequeue ERROR'.$e;
    }

    return ;
}


sub get_func_id {
    my ($class, $funcname) = @_;
    
    my $sth = $class->dbh->prepare(
        q{SELECT * FROM func WHERE name = ?}
    );

    $sth->execute( $funcname );
    my $row = $sth->fetchrow_hashref();
    my $func_id = $row ? $row->{id} : do{
        my $ins_sth = $class->dbh->prepare(
            q{INSERT INTO func ( name ) VALUES ( ? )}
        );
        $ins_sth->execute( $funcname );
        $sth->execute( $funcname );
        $sth->fetchrow_hashref->{id};
    };
    return $func_id;
}

=put
sub get_func_id {
    my ($class, $funcname) = @_;

    my $func = $class->find_or_create(
        'table' => 'func',
        'colum' => 'name',
        'value' => $funcname
    );
    return $func ? $func->{id} : undef;
}

sub find_or_create{
    my ($class , %content) = @_;

    my $sel = $class->dbh->prepare( qq/
        SELECT * FROM $content{table} WHERE $content{colum}  = ? /);
        
    $sel->execute( $content{value} );
    
    if ( $sel->rows ){
        return $sel->fetchrow_hashref();
    }
    else{
        my $ins = $class->dbh->prepare( qq/
            INSERT INTO $content{table} ( $content{colum}) VALUES (  ?) /);
        eval{
             $ins->execute( $content{value} );
        };
        if( my $e = $@ ){
            croak $e;
        }
        $sel->execute( $content{value} );

        if ( $sel->rows ){
            return $sel->fetchrow_hashref();
        }
    }
    return ;

}
=cut


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
        
    my $sel = $class->dbh->prepare( qq/
        SELECT * FROM $table / .
        ($q_where ? qq/ WHERE $q_where / : q// ).
        qq/ $q_opt / );

    $sel->execute();
    
    if ( $sel->rows ){
        my $ret_class = bless $sel->fetchrow_hashref , 'Ret::Class';
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

