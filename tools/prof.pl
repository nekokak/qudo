#! /usr/bin/perl
use strict;
use warnings;
use Qudo;

my $qudo = Qudo->new(
    driver_class => 'Skinny',
    databases    => [+{
        dsn      => 'dbi:SQLite:./tools/prof.db',
    }],
);
$qudo->enqueue('Worker::Test', { arg => 'test' });;

