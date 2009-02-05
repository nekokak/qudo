package Qudo::Driver::Skinny;

use DBIx::Skinny setup => +{
};

sub init_driver {
    my ($class, $qudo) = @_;

    $class->reconnect($qudo->{database});

    return $class;
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

