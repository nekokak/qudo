package Qudo::Hook::JSONArg;
use strict;
use warnings;
use base 'Qudo::Hook';
use JSON::XS;

sub load {
    my ($class, $manager) = @_;

    $manager->{hooks}->{pre_enqueue}->{'encode_json.arg'} = sub {
        my $args = shift;
        $args->{arg} = encode_json($args->{arg});
    };

    $manager->{hooks}->{pre_work}->{'decode_json.arg'} = sub {
        my $job = shift;
        $job->arg = decode_json($job->arg);
    };
}

sub unload {
    my ($class, $manager) = @_;

    delete $manager->{hooks}->{pre_enqueue}->{'encode_json.arg'};
    delete $manager->{hooks}->{pre_work}->{'decode_json.arg'};
}


1;

