package Qudo::Driver::DBI::DBD;
use strict;
use warnings;

sub new {
    my ($class, $dbd_type) =@_;
    die 'No Driver' unless $dbd_type;

    my $subclass = join '::', $class, $dbd_type;
    eval "use $subclass"; ## no critic
    die $@ if $@;
    bless {}, $subclass;
}

1;

