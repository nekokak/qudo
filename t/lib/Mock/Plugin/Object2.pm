package Mock::Plugin::Object2;
use strict;
use warnings;
use base 'Qudo::Plugin';

sub plugin_name { 'object2' }

sub load {
    my ($class, $option) = @_;
    $class->register(
        MyObj2->new($option)
    );
}

package MyObj2;
sub new {
    my ($class, $option) = @_;
    bless $option, $class;
}
sub call {
    my $self = shift;
    print STDOUT $self->{foo};
}

1;

