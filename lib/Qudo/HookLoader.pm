package Qudo::HookLoader;
use strict;
use warnings;
use UNIVERSAL::require;

sub register_hooks {
    my ($class, $to_class, $hook_modules) = @_;

    for my $module (@$hook_modules) {
        $module->require or Carp::croak $@;
        $module->load($to_class);
    }
}

sub unregister_hooks {
    my ($class, $from_class, $hook_modules) = @_;

    for my $module (@$hook_modules) {
        $module->unload($from_class);
    }
}

1;

