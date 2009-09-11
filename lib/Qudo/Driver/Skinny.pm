package Qudo::Driver::Skinny;

use DBIx::Skinny setup => +{
};

sub init_driver {
    my ($class, $master) = @_;

    $class->reconnect($master->{database});

    return $class;
}

sub exception_list {
    my ($class, %args) = @_;

    my $rs = $class->resultset(
        {
            select => [qw/exception_log.id
                          exception_log.func_id
                          exception_log.exception_time
                          exception_log.message
                          exception_log.uniqkey
                          exception_log.arg
                          exception_log.retried
                      /],
            from   => [qw/exception_log/],
            limit  => $args{limit},
            offset => $args{offset},
        }
    );

    if ($args{funcs}) {
        $rs->from([]);
        $rs->add_join(
            exception_log => {
                type      => 'inner',
                table     => 'func',
                condition => 'exception_log.func_id = func.id',
            }
        );
        $rs->add_where('func.name' => $args{funcs});
    }
    my $itr = $rs->retrieve;

    my @exception_list;
    while (my $row = $itr->next) {
        push @exception_list, $row->get_columns;
    }
    return \@exception_list;
}

sub job_status_list {
    my ($class, %args) = @_;

    my $rs = $class->resultset(
        {
            select => [qw/job_status.id
                          job_status.func_id
                          job_status.arg
                          job_status.uniqkey
                          job_status.status
                          job_status.process_time
                          job_status.job_end_time
                      /],
            from   => [qw/job_status/],
            limit  => $args{limit},
            offset => $args{offset},
        }
    );

    if ($args{funcs}) {
        $rs->from([]);
        $rs->add_join(
            job_status => {
                type      => 'inner',
                table     => 'func',
                condition => 'job_status.func_id = func.id',
            }
        );
        $rs->add_where('func.name' => $args{funcs});
    }
    my $itr = $rs->retrieve;

    my @job_status_list;
    while (my $row = $itr->next) {
        push @job_status_list, $row->get_columns;
    }
    return \@job_status_list;
}

sub job_count {
    my ($class, $funcs) = @_;

    my $rs = $class->resultset(
        {
            from => [qw/job/],
        }
    );
    $rs->add_select('COUNT(job.id)' => 'count');

    if ($funcs) {
        $rs->from([]);
        $rs->add_join(
            job => {
                type      => 'inner',
                table     => 'func',
                condition => 'job.func_id = func.id',
            }
        );
        $rs->add_where('func.name' => $funcs);
    }

    return $rs->retrieve->first->count;
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

    my $servertime = $class->get_server_time;
    $rs->add_where('job.grabbed_until' => { '<=', => $servertime});
    $rs->add_where('job.run_after'     => { '<=', => $servertime});

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

    return;
}

sub set_job_status {
    my ($class, $args) = @_;

    $class->insert('job_status', $args);

    return;
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

sub retry_from_exception_log {
    my ($class, $exception_log_id) = @_;

    $class->update('exception_log',
        {
            retried => 1,
        },
        {
            id => $exception_log_id,
        },
    );
}

1;

