package Mock::Hook::PostWork;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'post_work' }

sub load {
    my ($class, $manager) = @_;

    $manager->{hooks}->{post_work}->{'stdout'} = sub {
        my $args = shift;
        print STDOUT "Worker::Test: post worked!\n";
    };
}

sub unload {
    my ($class, $manager) = @_;

    delete $manager->{hooks}->{post_work}->{'stdout'};
}

1;

