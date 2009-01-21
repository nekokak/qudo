package Qudo::Model::Row::Job;
use strict;
use warnings;
use base 'DBIx::Skinny::Row';

sub _func {
    my $self = shift;
    $self->{_func} ||= $self->skinny->single('func',{id => $self->func_id});
}

sub funcname {
    my $self = shift;
    my $func = $self->_func;
    $func ? $func->name : undef;
}

sub funcid {
    my $self = shift;
    my $func = $self->_func;
    $func ? $func->id : undef;
}

sub dequeue {
    my $self = shift;
    $self->delete;
}

sub completed {
    my $self = shift;
    $self->{_is_completed} = 1;
    $self->dequeue;
}

sub is_completed {
    my $self = shift;
    $self->{_is_completed};
}

sub failed {
    my ($self, $msg) = @_;
    $self->skinny->insert('exception_log',
        {
            func_id => $self->funcid,
            job_id  => $self->id,
            message => $msg,
        }
    );
}

1;

