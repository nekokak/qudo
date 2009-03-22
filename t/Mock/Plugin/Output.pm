package Mock::Plugin::Output;
use strict;
use warnings;
use base 'Qudo::Plugin';

sub plugin_name { 'output' }

sub load {
    my $class = shift;
    $class->register(
        sub {
            my $val = shift;
            print STDOUT $val;
        }
    );
}

1;

