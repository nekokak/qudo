package Mock::Hook::PreWork;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'pre_work' }

sub load {
    my $class = shift;
    $class->register(
        sub {
            my $args = shift;
            print STDOUT "Worker::Test: pre worked!\n";
        }
    );
}

sub unload { hook_point() }

1;

