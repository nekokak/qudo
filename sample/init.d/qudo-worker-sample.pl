#! /usr/bin/perl
use strict;
use warnings;
use lib qw(/path/to/lib);
use UNIVERSAL::require;
use Sys::Syslog;
use Parallel::Prefork;
use Perl6::Say;
use Carp;
use Sub::Throttle qw/throttle/;
use Module::Find;

sub MaxRequestsPerChild () { 30 }

openlog("Project::Worker($$)", 'cons,pid', 'local6');
syslog('info', "start project's qudo @ARGV");

my @workers = @ARGV;
unless (@workers) {
    @workers = Module::Find::findallmod('Project::Worker::Qudo');
}

for my $worker (@workers) {
    print "Setting up the $worker\n";
    $worker->use or die $@;
}

say "START WORKING : $$";

my $pm = Parallel::Prefork->new({
    max_workers  => $ENV{QUDO_TEST} ? 1 : 5,
    fork_delay   => 1,
    trap_signals => {
        TERM => 'TERM',
        HUP  => 'TERM',
    },
});

while ($pm->signal_received ne 'TERM') {
    $pm->start and next;

    say "spawn $$";

    {
        require Project::Qudo;
        my $manager = Project::Qudo->new->manager;
        for my $worker (@workers) {
            $manager->can_do($worker);
        }

        my $reqs_before_exit = MaxRequestsPerChild;
        $SIG{TERM} = sub { $reqs_before_exit = 0 };
        while ($reqs_before_exit > 0) {
            if (throttle(0.5, sub { $manager->work_once })) {
                say "work $$";
                --$reqs_before_exit
            } else {
                sleep 3
            }
        }
    }

    say "FINISHED $$";
    $pm->finish;
}

$pm->wait_all_children;

warn "should not reach to here ;-)";

