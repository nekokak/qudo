package Qudo::Plugin;
use strict;
use warnings;

sub plugin_name {
    warn 'this method is abstract';
}

sub register {
    my ($class, $code) = @_;
    $class->plugin_name => $code;
}

sub load {
    warn 'this method is abstract';
}

1;

