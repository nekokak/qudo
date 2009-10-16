package Mock::Plugin::Object;
use strict;
use warnings;
use base 'Qudo::Plugin';

sub plugin_name { 'object' }

sub load {
    my $class = shift;
    $class->register(
        MyObj->new
    );
}

package MyObj;
sub new {
    my $class = shift;
    bless {}, $class;
}
sub stdout {
    my ($self, $val) = @_;
    print STDOUT $val;
}

1;

