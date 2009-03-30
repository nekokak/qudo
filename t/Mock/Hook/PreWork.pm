package Mock::Hook::PreWork;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'pre_work' }

sub load {
    my ($class, $manager) = @_;

    $manager->{hooks}->{pre_work}->{stdout} = sub {
        my $args = shift;
        print STDOUT "Worker::Test: pre worked!\n";
    };
}

sub unload {
    my ($class, $manager) = @_;

    delete $manager->{hooks}->{pre_work}->{stdout};

}

1;

