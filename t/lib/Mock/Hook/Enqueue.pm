package Mock::Hook::Enqueue;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'pre_enqueue' }

sub load {
    my ($class, $klass) = @_;

    $klass->hooks->{pre_enqueue}->{'enqueue'} = sub {
        my $args = shift;
        $args->{arg} = 'hooook';
    };
}

sub unload {
    my ($class, $klass) = @_;

    delete $klass->hooks->{pre_enqueue}->{'enqueue'};
}

1;

