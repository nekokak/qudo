#! /usr/bin/perl
use strict;
use warnings;
use Benchmark qw/cmpthese/;
use Qudo;


my $s_qudo = Qudo->new(
    driver_class => 'Skinny',
    databases    => [+{
        dsn      => 'dbi:SQLite:./tools/prof.db',
    }],
);

$s_qudo->driver->do('delete from job');

my $d_qudo = Qudo->new(
    driver_class => 'DBI',
    databases    => [+{
        dsn      => 'dbi:SQLite:./tools/prof.db',
    }],
);

cmpthese(1000, {
    'D::DBI'                 => sub {$d_qudo->enqueue('Worker::Test', { arg => 'test' })},
    'D::DBI#suppress_job'    => sub {$d_qudo->enqueue('Worker::Test', { arg => 'test', suppress_job => 1 })},
    'D::Skinny'              => sub {$s_qudo->enqueue('Worker::Test', { arg => 'test' })},
    'D::Skinny#suppress_job' => sub {$s_qudo->enqueue('Worker::Test', { arg => 'test', suppress_job => 1 })},
});

$s_qudo->driver->do('delete from job');

__END__
                         Rate D::Skinny D::Skinny#suppress_job D::DBI D::DBI#suppress_job
D::Skinny               787/s        --                   -35%   -36%                -47%
D::Skinny#suppress_job 1220/s       55%                     --    -1%                -18%
D::DBI                 1235/s       57%                     1%     --                -17%
D::DBI#suppress_job    1493/s       90%                    22%    21%                  --

