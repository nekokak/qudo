package Qudo::Driver::DBI::DBD::mysql;
use strict;
use warnings;

sub last_insert_id { $_[2]->{mysql_insertid} || $_[2]->{insertid} }

sub sql_for_unixtime {
    return "UNIX_TIMESTAMP()";
}

1;

