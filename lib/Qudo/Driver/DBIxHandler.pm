package Qudo::Driver::DBIxHandler;
use strict;
use warnings;

use DBI qw/:sql_types/;
use DBIx::Handler;

sub init_driver {
    my ($class, $master) = @_;

    for my $database (@{$master->{databases}}) {
        my $connection = $class->new($database);
        $master->set_connection($database->{dsn}, $connection);
    }
}

sub schema {
    return (
        job           => { pk => [qw/id/], columns => [qw/id arg uniqkey func_id grabbed_until retry_cnt priority/]   },
        func          => { pk => [qw/id/], columns => [qw/id func_id message arg exception_time retried/]             },
        exception_log => { pk => [qw/id/], columns => [qw/id func_id exception_time message uniqkey arg retried/]     },
        job_status    => { pk => [qw/id/], columns => [qw/id func_id arg uniqkey status job_start_time job_end_time/] },
    );
}

sub init_schema {
    my $self = shift;

    my $schema = $self->{schema};
    my $dbh = $self->{handler}->dbh;

    for my $table_name (keys %$schema) {
        my $table = $schema->{$table_name};
        $table->{name} = $table_name;
        $table->{quoted_name} = $dbh->quote_identifier($table_name);

        my @quoted_pk;
        for my $pk (@{ $table->{pk} }) {
            push @quoted_pk => $dbh->quote_identifier($pk);
        }
        $table->{quoted_pk} = \@quoted_pk;

        my @quoted_columns;
        my %quoted_columns_map;
        for my $column (@{ $table->{columns} }) {
            my $quoted = $dbh->quote_identifier($column);
            push @quoted_columns => $quoted;
            $quoted_columns_map{$column} = $quoted;
        }
        $table->{quoted_columns} = \@quoted_columns;
        $table->{quoted_columns_map} = \%quoted_columns_map;
    }

    return $self;
}

sub new {
    my ($class, $database) = @_;
    my ($dsn, $user, $pass, $attr, $opt)
        = @$database{qw/dsn username password connect_options handler_options/};

    my $handler = DBIx::Handler->new($dsn, $user, $pass, $attr, $opt);
    my %schema  = $class->init_schema();
    my $self = bless {
        handler    => $handler,
        schema     => \%schema,
        _sql_cache => {},
    } => $class;
    $self->init_schema();
}

# tiny sql builder
sub _select_sql {
    my ($self, $table_name, $where, $opt) = @_;
    $opt ||= {};

    my $table = $self->{schema}->{$table_name};
    my $quoted_columns_map = $table->{quoted_columns_map};

    my $columns = join ', ', @{ $table->{quoted_columns} };
    my $sql     = qq{SELECT $columns FROM }.$table->{quoted_name};

    my @bind;
    if (defined $where) {
        my $where_sql = '';
        for my $column (sort keys %$where) {
            $where_sql .= ' AND' if $where_sql;

            my $quoted = $quoted_columns_map->{$column};
            my $value  = $where->{$column};
            if (ref $value eq 'ARRAY') {
                if (my $size = @$value) {
                    $where_sql .= ' '.$quoted.' IN ('.join(', ', ('?') x $size).')';
                    push @bind => @$value;
                }
                else {
                    $where_sql .= ' 1=0'; # empty
                }
            }
            elsif (not ref $value) {
                $where_sql .= " $quoted = ?";
            }
            else {
                die "Unknown type: $value";
            }
        }
        $sql .= $where_sql;
    }

    if (exists $opt->{order_by}) {
        my $table = $self->{schema}->{job};
        my ($column, $order) = %{ $opt->{order_by} };
        die "Invalid order: $order" if $order ne 'DESC' and $order ne 'ASC';

        my $quoted = $table->{quoted_columns_map}->{$column};
        $sql .= " ORDER BY $quoted $order";
    }

    $sql .= sprintf ' LIMIT %d',  $opt->{limit}  if exists $opt->{limit};
    $sql .= sprintf ' OFFSET %d', $opt->{offset} if exists $opt->{offset};

    return $sql, \@bind;
}

sub _lookup_job_sql {
    my $self = shift;
    $self->{_sql_cache}->{_lookup_job_sql} ||= do {
        my ($sql) = $self->_select_sql(job => {
            id => 0,
        }, {
            order_by => { priority => 'DESC' },
            limit    => 1,
        });
        $sql;
    };
}

sub lookup_job {
    my ($self, $job_id) = @_;
    my $dbh = $self->{handler}->dbh;

    my $sql = $self->_lookup_job_sql();
    my $sth = $dbh->prepare_cached($sql);
    $sth->bind_param(1, $job_id, SQL_INTEGER);
    $sth->execute();
    return $self->_get_job_data($sth);
}

sub find_job {
    my ($self, $limit, $func_ids) = @_;
    my $dbh = $self->{handler}->dbh;

    my $servertime = $self->_get_server_time($dbh);
    my ($sql, $bind) = $self->_select_sql(job => {
        func_id       => $func_ids,
        grabbed_until => \['<= ?', $servertime],
        run_after     => \['<= ?', $servertime],
    }, {
        order_by => { priority => 'DESC' },
        limit    => $limit,
    });

    my $sth = $dbh->prepare_cached($sql);
    for my $i (1..@$bind) {
        $sth->bind_param($i, $bind->[$i-1], SQL_INTEGER);
    }
    $sth->execute();
    return $self->_get_job_data($sth);
}

sub _get_job_data {
    my ($self, $sth) = @_;
    sub {
        my $job = $self->_fetch_row_by_sth(job => $sth);
        unless (defined $job) {
            $sth->finish();
            return;
        }

        return +{
            job_id            => $job->{id},
            job_arg           => $job->{arg},
            job_uniqkey       => $job->{uniqkey},
            job_grabbed_until => $job->{grabbed_until},
            job_retry_cnt     => $job->{retry_cnt},
            job_priority      => $job->{priority},
            func_id           => $job->{func_id},
        };
    };
}

sub _grab_a_job_sql {
    my $self = shift;
    $self->{_sql_cache}->{_grab_a_job_sql} ||= do {
        my $table = $self->{schema}->{job};

        my $table_name    = $table->{quoted_name};
        my $id            = $table->{quoted_columns_map}->{id};
        my $grabbed_until = $table->{quoted_columns_map}->{grabbed_until};
        qq{UPDATE $table_name SET $grabbed_until = ? WHERE $id = ? AND $grabbed_until = ?};
    };
}


sub grab_a_job {
    my ($self, %args) = @_;
    my $sql = $self->_grab_a_job_sql();

    my $dbh = $self->{handler}->dbh;
    my $sth = $dbh->prepare_cached($sql);
    $sth->bind_param(1, $args{grabbed_until},     SQL_INTEGER);
    $sth->bind_param(2, $args{job_id},            SQL_INTEGER);
    $sth->bind_param(3, $args{old_grabbed_until}, SQL_INTEGER);
    $sth->execute();
}

sub _logging_exception_sql {
    my $self = shift;
    $self->{_sql_cache}->{_logging_exception_sql} ||= do {
        my $table = $self->{schema}->{exception_log};

        my $table_name = $table->{quoted_name};
        my $columns    = $table->{quoted_columns};
        'INSERT INTO '.$table_name.' ('.$columns.') VALUES ('.join(', ', ('?') x @$columns).')';
    };
}

sub logging_exception {
    my ($self, $args) = @_;
    my $sql = $self->_logging_exception_sql();

    my $dbh = $self->{handler}->dbh;
    my $sth = $dbh->prepare_cached($sql);

    my $i = 1;
    for my $column (@{ $self->{schema}->{exception_log}->{columns} }) {
        my $value = $args->{$column};

        $sth->bind_param($i, , SQL_INTEGER);
    }
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

