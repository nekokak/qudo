package Qudo::Driver::DBI::DBD::SQLite;
use strict;
use warnings;

sub last_insert_id { $_[1]->func('last_insert_rowid') }

sub sql_for_unixtime { return time() }

1;

