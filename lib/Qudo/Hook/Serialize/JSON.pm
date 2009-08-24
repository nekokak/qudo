package Qudo::Hook::Serialize::JSON;
use strict;
use warnings;
use base 'Qudo::Hook';
use JSON::XS;

sub load {
    my ($class, $klass) = @_;

    $klass->hooks->{serialize}->{json} = sub {
        my $args = shift;
        $args->{arg} = encode_json($args->{arg});
    };

    $klass->hooks->{deserialize}->{json} = sub {
        my $job = shift;
        $job->arg_origin = $job->arg;
        $job->arg = decode_json($job->arg);
    };
}

sub unload {
    my ($class, $klass) = @_;

    delete $klass->hooks->{serialize}->{json};
    delete $klass->hooks->{deserialize}->{json};
}


1;

