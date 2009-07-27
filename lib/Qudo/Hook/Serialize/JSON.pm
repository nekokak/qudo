package Qudo::Hook::Serialize::JSON;
use strict;
use warnings;
use base 'Qudo::Hook';
use JSON::XS;

sub load {
    my ($class, $manager) = @_;

    $manager->{hooks}->{serialize}->{json} = sub {
        my $args = shift;
        $args->{arg} = encode_json($args->{arg});
    };

    $manager->{hooks}->{deserialize}->{json} = sub {
        my $job = shift;
        $job->arg_origin = $job->arg;
        $job->arg = decode_json($job->arg);
    };
}

sub unload {
    my ($class, $manager) = @_;

    delete $manager->{hooks}->{serialize}->{json};
    delete $manager->{hooks}->{deserialize}->{json};
}


1;

