package Mock::Hook::Enqueue;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'pre_enqueue' }

sub load {
    my ($class, $manager) = @_;

    $manager->{hooks}->{pre_enqueue}->{'enqueue'} = sub {
        my $args = shift;
        $args->{arg} = 'hooook';
    };
}

sub unload {
    my ($class, $manager) = @_;

    delete $manager->{hooks}->{pre_enqueue}->{'enqueue'};
}

1;

