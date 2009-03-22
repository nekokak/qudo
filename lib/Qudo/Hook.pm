package Qudo::Hook;
use strict;
use warnings;

sub register {
    my ($class, $code) = @_;

    die '$code is not coderef' unless ref $code eq 'CODE';
    $class->hook_point => $code;
}

sub hook_point {
    warn 'this method is abstract';
}

sub load {
    warn 'this method is abstract';
}

sub unload {
    warn 'this method is abstract';
}

1;

