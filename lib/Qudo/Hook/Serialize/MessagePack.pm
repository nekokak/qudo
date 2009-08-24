package Qudo::Hook::Serialize::MessagePack;
use strict;
use warnings;
use base 'Qudo::Hook';
use Data::MessagePack;

sub load {
    my ($class, $klass) = @_;

    $klass->hooks->{serialize}->{messagepack} = sub {
        my $args = shift;
        $args->{arg} = Data::MessagePack->pack($args->{arg});
    };

    $klass->hooks->{deserialize}->{messagepack} = sub {
        my $job = shift;
        $job->arg_origin = $job->arg;
        $job->arg = Data::MessagePack->unpack($job->arg);
    };
}

sub unload {
    my ($class, $klass) = @_;

    delete $klass->hooks->{serialize}->{messagepack};
    delete $klass->hooks->{deserialize}->{messagepack};
}


1;

