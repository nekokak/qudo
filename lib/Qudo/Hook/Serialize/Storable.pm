package Qudo::Hook::Serialize::Storable;
use strict;
use warnings;
use base 'Qudo::Hook';
use Storable ();

sub load {
    my ($class, $klass) = @_;

    $klass->hooks->{serialize}->{storable} = sub {
        my $args = shift;
        $args->{arg} = Storable::nfreeze($args->{arg});
    };

    $klass->hooks->{deserialize}->{storable} = sub {
        my $job = shift;
        $job->arg_origin = $job->arg;
        my $arg = $job->arg;
        my $ref = ref $arg;
        if ($ref eq 'SCALAR') {
            $job->arg = Storable::thaw($$arg);
        } else {
            $job->arg = Storable::thaw($arg);
        }
    };
}

sub unload {
    my ($class, $klass) = @_;

    delete $klass->hooks->{serialize}->{storable};
    delete $klass->hooks->{deserialize}->{storable};
}


1;

