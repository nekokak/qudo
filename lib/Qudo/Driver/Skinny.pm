package Qudo::Driver::Skinny;

use DBIx::Skinny setup => +{
};

sub init_driver {
    my ($class, $qudo) = @_;

    $class->reconnect($qudo->{database});

    return $class;
}

1;

