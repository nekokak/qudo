package Qudo::Driver::Skinny;

use DBIx::Skinny setup => +{
};

sub init_driver {
    my ($class, $master) = @_;

    $class->reconnect($master->{database});

    return $class;
}

sub job_list {
    my ($class, $limit, $funcs) = @_;

    my $rs = $class->_search_job_rs(limit => $limit);
    if ($funcs) {
        $rs->add_where('func.name' => $funcs)
    }

    my $itr = $rs->retrieve;
    my $code = $class->_get_job_data($itr);

    my @jobs;
    while (1) {
        my $row = $code->();
        last unless $row;
        push @jobs, $row;
    }
    return \@jobs;
}

sub lookup_job {
    my ($class, $job_id) = @_;

    my $rs = $class->_search_job_rs(limit => 1);
    $rs->add_where('job.id' => $job_id);
    my $itr = $rs->retrieve;

    return $class->_get_job_data($itr);
}

sub find_job {
    my ($class, $limit, $func_map) = @_;

    my $rs = $class->_search_job_rs(limit => $limit);
    $rs->add_where('func.name' => [keys %$func_map]);
    my $itr = $rs->retrieve;

    return $class->_get_job_data($itr);
}

sub _search_job_rs {
    my ($class, %args) = @_;

    my $rs = $class->resultset(
        {
            select => [qw/job.id job.arg job.uniqkey job.func_id job.grabbed_until job.retry_cnt/],
            limit  => $args{limit},
        }
    );
    $rs->add_select('func.name' => 'funcname');
    $rs->add_join(
        job => {
            type      => 'inner',
            table     => 'func',
            condition => 'job.func_id = func.id',
        }
    );
    return $rs;
}

sub _get_job_data {
    my ($class, $itr) = @_;
    sub {
        my $job = $itr->next or return;
        return +{
            job_id            => $job->id,
            job_arg           => $job->arg,
            job_uniqkey       => $job->uniqkey,
            job_grabbed_until => $job->grabbed_until,
            job_retry_cnt     => $job->retry_cnt,
            func_id           => $job->func_id,
            func_name         => $job->funcname,
        };
    };
}

sub grab_a_job {
    my ($class, %args) = @_;

    return $class->update('job',
        {
            grabbed_until => $args{grabbed_until},
        },
        {
            id            => $args{job_id},
            grabbed_until => $args{old_grabbed_until},
        }
    );

}

sub logging_exception {
    my ($class, $args) = @_;

    $class->insert('exception_log', $args);
}

sub get_server_time {
    my $class = shift;
    my $unixtime_sql = $class->dbd->sql_for_unixtime;
    return $class->dbh->selectrow_array("SELECT $unixtime_sql");
}

sub enqueue {
    my ($class, $args) = @_;
    my $job = $class->insert('job', $args);
    return $job ? $job->id : undef;
}

sub reenqueue {
    my ($class, $job_id, $args) = @_;

    $class->update('job', $args, {id => $job_id});
}

sub dequeue {
    my ($class, $args) = @_;
    $class->delete('job', $args);
}

sub get_func_id {
    my ($class, $funcname) = @_;

    my $func = $class->find_or_create('func',{ name => $funcname });
    return $func ? $func->id : undef;
}

1;

