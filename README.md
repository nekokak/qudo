# NAME

Qudo - simple and extensible job queue manager

# SYNOPSIS

    # enqueue job:
    use Qudo;
    my $qudo = Qudo->new(
        driver_class => 'Skinny', # optional.
        databases => [+{
            dsn      => 'dbi:SQLite:/tmp/qudo.db',
            username => '',
            password => '',
        }],
    );
    $qudo->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey'});
    
    # do work:
    use Qudo;
    my $qudo2 = Qudo->new(
        driver_class => 'Skinny', # optional.
        databases => [+{
            dsn      => 'dbi:SQLite:/tmp/qudo.db',
            username => '',
            password => '',
        }],
        manager_abilities => [qw/Worker::Test/],
    );
    $qudo2->work(); # boot manager
    # work work work!

# DESCRIPTION

Qudo is simple and extensible job queue manager system.

Your application can insert job into DB ,that is managed by Qudo.
And Your application can get & execute job by Qudo worker.
Qudo corresponds to deal with DB as MySQL and SQLite.

If you add Hook Point around job's working method ,
you can add it easily and many point of work milestone.
Qudo is consided about adding Hook Point Flexibility.

# USEAGE

## `Qudo->new( %args )`

Optional members of `%args` are:

- `driver_class`

    set Qudo::Driver::(Skinny|DBI).
    default driver\_class is Skinny.

- `databases`

    An arrayref of database information. Qudo can use multiple databases,
    such that if any of them are unavailable,
    the worker will search for appropriate jobs in the other databases automatically.

    Each member of the `databases` value should be a hashref containing either:

    - `dsn`

        The database DSN for this database.

    - `username`

        The username to use when connecting to this database.

    - `password`

        The password to use when connecting to this database.

- `manager_abilities`

    An arrayref of worker class name.
    please specify it when moving it by the usage of worker.
    it is not necessary to specify it for the usage of enqueue client.

- `find_job_limit_size`

    The maximum number in which it looks for job by one processing.
    Qudo default limit 30.
    please specify it when moving it by the usage of worker.
    it is not necessary to specify it for the usage of enqueue client.

- `retry_seconds`

    The number of seconds after which to try reconnecting to apparently dead databases.
    If not given, Qudo will retry connecting to databases after 30 seconds.

- `default_hooks`

    An arrayref of hook class name.

- `default_plugins`

    An arrayref of plugin class name.

## `Qudo->manager`

get Qudo::Manager instance.
see [Qudo::Manager](https://metacpan.org/pod/Qudo::Manager)

## `Qudo->enqueue( %args )`

see [Qudo::Manager](https://metacpan.org/pod/Qudo::Manager) enqueue method.

## `Qudo->work( %args )`

Find and perform any jobs $manager can do, forever.

When no job is available, the working process will sleep for $delay  seconds (or 5, if not specified) before looking again.

## `Qudo->job_count( $funcname, $dsn )`

Returns a job count infomations.
The required arguments :

- `funcname`

    the name of the function or a reference to an array of functions.

- `dsn`

    The database DSN for job count target database.

## `Qudo->exception_list( $args, $dsn )`

Returns a job exception infomations.
Optional members of `$args` are:

- args
    - limit

        get exception log limit size.
        default by 10.

    - offset

        get exception log offset size.
        default by 0.

- `dsn`

    The database DSN for job count target database.

## `Qudo->job_status_list( $args, $dsn )`

Returns a job exception infomations.
Optional members of `$args` are:

- args
    - limit

        get job\_status log limit size.
        default by 10.

    - offset

        get job\_status log offset size.
        default by 0.

- `dsn`

    The database DSN for job count target database.

# REPOS

http://github.com/nekokak/qudo/tree/master

# AUTHOR

Atsushi Kobayashi &lt;nekokak \_at\_ gmail dot com>

Masaru Hoshino &lt;masartz \_at\_ gmail dot com>

# COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.
