package Qudo::Driver::Skinny;

use DBIx::Skinny setup => +{
};

sub init_driver {
    my ($class, $master) = @_;

    $class->reconnect($master->{database});

    return $class;
}

sub lookup_job {
    my ($class, $job_id) = @_;

    my $job_itr = $class->search_named(q{
        SELECT
            job.id, job.arg, job.uniqkey, job.func_id,
            job.grabbed_until,
            func.name AS funcname
        FROM
            job, func
        WHERE
            job.func_id = func.id AND
            job.id      = :job_id
        LIMIT 1
    },{job_id => $job_id});

    return $class->_get_job_data($job_itr);
}

sub find_job {
    my ($class, $limit, $func_map) = @_;

    my ($sql_part, $ids) = $class->_func_map_ids($func_map);
    my $job_itr = $class->search_named(q{
        SELECT
            job.id,  job.arg, job.uniqkey, job.func_id,
            job.grabbed_until,
            func.name AS funcname
        FROM
            job, func
        WHERE
            job.func_id = func.id AND
            func.id in (%s)
        LIMIT %d 
    },{%$ids},[$sql_part, $limit]);

    return $class->_get_job_data($job_itr);
}

sub _func_map_ids {
    my ($class, $func_map) = @_;

    my (%ids, @parts);
    for my $funcname (keys %$func_map) {
        my $func = $class->single('func',{name => $funcname});
        next unless $func;

        (my $part = $funcname) =~ s/:/_/g;
        push @parts, $part;

        $ids{$part} = $func->id;
    }

    my $sql_part = join ',', map { ':'.$_ } @parts;
    return $sql_part, \%ids;
}

sub _get_job_data {
    my ($class, $itr) = @_;
    sub {
        my $job = $itr->next;
        return +{
            job_id            => $job->id,
            job_arg           => $job->arg,
            job_uniqkey       => $job->uniqkey,
            job_grabbed_until => $job->grabbed_until,
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

sub dequeue {
    my ($class, $args) = @_;
    $class->delete->('job', $args);
}

sub get_func_id {
    my ($class, $funcname) = @_;

    my $func = $class->find_or_create('func',{ name => $funcname });
    return $func ? $func->id : undef;
}

1;

