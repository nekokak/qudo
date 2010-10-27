#! /usr/bin/perl
use strict;
use warnings;
use Benchmark qw/countit timethese timeit timestr/;
use Qudo;

my $driver = shift || 'Skinny';
my $qudo = Qudo->new(
    driver_class => $driver,
    databases    => [+{
        dsn      => 'dbi:SQLite:./tools/prof.db',
    }],
);

my $t = countit 2 => sub {
    $qudo->enqueue('Worker::Test', { arg => 'test' });;
};

print timestr($t), "\n";

__END__
2 wallclock secs ( 1.96 usr +  0.10 sys =  2.06 CPU) @ 1406.31/s (n=2897)
