package Qudo::Hook::Serialize::MessagePack;
use strict;
use warnings;
use base 'Qudo::Hook';
use Data::MessagePack;

sub load {
    my ($class, $manager) = @_;

    $manager->{hooks}->{serialize}->{messagepack} = sub {
        my $args = shift;
        $args->{arg} = Data::MessagePack->pack($args->{arg});
    };

    $manager->{hooks}->{deserialize}->{messagepack} = sub {
        my $job = shift;
        $job->arg_origin = $job->arg;
        $job->arg = Data::MessagePack->unpack($job->arg);
    };
}

sub unload {
    my ($class, $manager) = @_;

    delete $manager->{hooks}->{serialize}->{messagepack};
    delete $manager->{hooks}->{deserialize}->{messagepack};
}


1;

