package Mock::Hook::PostWork;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'post_work' }

sub load {
    my $class = shift;
    $class->register(
        sub {
            my $args = shift;
            print STDOUT "Worker::Test: post worked!\n";
        }
    );
}

sub unload { hook_point() }

1;

