package Mock::Hook::Enqueue;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'pre_enqueue' }

sub load {
    my $class = shift;
    $class->register(
        sub {
            my $args = shift;
            $args->{arg} = 'hooook';
        }
    );
}

sub unload { hook_point() }

1;

