package Mock::Hook::PreWork;
use strict;
use warnings;
use base 'Qudo::Hook';

sub hook_point { 'pre_work' }

sub load {
    my ($class, $klass) = @_;

    $klass->hooks->{pre_work}->{stdout} = sub {
        my $args = shift;
        print STDOUT "Worker::Test: pre worked!\n";
    };
}

sub unload {
    my ($class, $klass) = @_;

    delete $klass->hooks->{pre_work}->{stdout};

}

1;

