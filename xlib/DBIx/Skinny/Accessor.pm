package DBIx::Skinny::Accessor;
use strict;
use warnings;

sub import {
    my $caller = caller;

    {
        no strict 'refs';
        *{"$caller\::new"} = \&_new;
        *{"$caller\::mk_accessors"} = \&_mk_accessors;
    }
}

sub _new {
    my ($class, $args) = @_;
    $args ||= {};
    my $self = bless {%$args}, $class;

    if ( $class->can('init') ) {
        $self->init;
    }

    return $self;
}

sub _mk_accessors {
    my $caller = caller;

    {
        no strict 'refs';
        for my $n (@_) {
            *{"$caller\::$n"} = __m($n);
        }
    }
}

sub __m {
    my $n = shift;
    sub {
        return $_[0]->{$n} if @_ == 1;
        return $_[0]->{$n} = $_[1] if @_ == 2;
        shift->{$n} = \@_;
    };
}

'base code from Class::Accessor::Lite';
