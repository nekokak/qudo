package Qudo::Driver::Skinny;

use strict;
use warnings;

use DBIx::Skinny;

sub init_driver {
    my ($class, $master) = @_;

    for my $database (@{$master->{databases}}) {
        my $connection = $class->new($database);
        $master->set_connection($database->{dsn}, $connection);
    }
}

sub lookup_job {
    my ($self, $job_id) = @_;

    my $rs = $self->_search_job_rs(limit => 1);

    $rs->add_where('job.id' => $job_id);

    my $itr = $rs->retrieve('job');

    return $self->_get_job_data($itr);
}

sub find_job {
    my ($self, $limit, $func_ids) = @_;

    my $rs = $self->_search_job_rs(limit => $limit);

    $rs->add_where('job.func_id' => $func_ids);

    my $servertime = $self->get_server_time;
    $rs->add_where('job.grabbed_until' => { '<=', => $servertime});
    $rs->add_where('job.run_after'     => { '<=', => $servertime});

    my $itr = $rs->retrieve('job');

    return $self->_get_job_data($itr);
}

sub _search_job_rs {
    my ($self, %args) = @_;

    my $rs = $self->resultset(
        {
            select => [qw/job.id job.arg job.uniqkey job.func_id job.grabbed_until job.retry_cnt job.priority/],
            from   => 'job',
            limit  => $args{limit},
        }
    );
    $rs->order({column => 'job.priority', desc => 'DESC'});

    return $rs;
}

sub _get_job_data {
    my ($self, $itr) = @_;
    sub {
        my $job = $itr->next or return;
        return +{
            job_id            => $job->id,
            job_arg           => $job->arg,
            job_uniqkey       => $job->uniqkey,
            job_grabbed_until => $job->grabbed_until,
            job_retry_cnt     => $job->retry_cnt,
            job_priority      => $job->priority,
            func_id           => $job->func_id,
        };
    };
}

sub grab_a_job {
    my ($self, %args) = @_;

    return $self->update('job',
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
    my ($self, $args) = @_;
    $self->insert('exception_log', $args);
    return;
}

sub set_job_status {
    my ($self, $args) = @_;
    $self->insert('job_status', $args);
    return;
}

sub get_server_time {
    my $self = shift;
    my $unixtime_sql = $self->dbd->sql_for_unixtime;
    return $self->dbh->selectrow_array("SELECT $unixtime_sql");
}

sub enqueue {
    my ($self, $args) = @_;
    my $job = $self->insert('job', $args);
    return $job ? $job->id : undef;
}

sub reenqueue {
    my ($self, $job_id, $args) = @_;
    $self->update('job', $args, {id => $job_id});
}

sub dequeue {
    my ($self, $args) = @_;
    $self->delete('job', $args);
}

sub func_from_name {
    my ($self, $funcname) = @_;
    my $row = $self->find_or_create('func',{ name => $funcname });
    return { id => $row->id, name => $row->name };
}

sub func_from_id {
    my ($self, $funcid) = @_;
    my $row = $self->single('func',{ id => $funcid });
    return { id => $row->id, name => $row->name };
}

sub retry_from_exception_log {
    my ($self, $exception_log_id) = @_;

    $self->update('exception_log',
        {
            retried => 1,
        },
        {
            id => $exception_log_id,
        },
    );
}

sub exception_list {
    my ($self, $args) = @_;

    my $rs = $self->resultset(
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
            limit  => $args->{limit},
            offset => $args->{offset},
        }
    );

    if ($args->{funcs}) {
        $rs->from([]);
        $rs->add_join(
            exception_log => {
                type      => 'inner',
                table     => 'func',
                condition => 'exception_log.func_id = func.id',
            }
        );
        $rs->add_where('func.name' => $args->{funcs});
    }
    my $itr = $rs->retrieve;

    my @exception_list;
    while (my $row = $itr->next) {
        push @exception_list, $row->get_columns;
    }
    return \@exception_list;
}

sub job_status_list {
    my ($self, $args) = @_;

    my $rs = $self->resultset(
        {
            select => [qw/job_status.id
                          job_status.func_id
                          job_status.arg
                          job_status.uniqkey
                          job_status.status
                          job_status.job_start_time
                          job_status.job_end_time
                      /],
            from   => [qw/job_status/],
            limit  => $args->{limit},
            offset => $args->{offset},
        }
    );

    if ($args->{funcs}) {
        $rs->from([]);
        $rs->add_join(
            job_status => {
                type      => 'inner',
                table     => 'func',
                condition => 'job_status.func_id = func.id',
            }
        );
        $rs->add_where('func.name' => $args->{funcs});
    }
    my $itr = $rs->retrieve;

    my @job_status_list;
    while (my $row = $itr->next) {
        push @job_status_list, $row->get_columns;
    }
    return \@job_status_list;
}

sub job_count {
    my ($self, $funcs) = @_;

    my $rs = $self->resultset(
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

1;

