package Qudo;
use strict;
use warnings;

our $VERSION = '0.01';

use Qudo::Model;
use Carp;

our $RETRY_SECONDS = 30;

sub new {
    my ($class, %args) = @_;

    my $self = bless {}, $class;

    croak "database must be an hashref if specified"
        unless !exists $args{database} || ref $args{database} eq 'HASH';
    my $database = delete $args{database};

    $self->{retry_seconds} = delete $args{retry_seconds} || $RETRY_SECONDS;

    Qudo::Model->connect_info($database);

    return $self;
}

sub lookup_job {}
sub list_jobs {}

sub enqueue {
    my ($self, $funcname, $arg, $uniqkey) = @_;

    # hook
    my $func_id = Qudo::Model->find_or_create('func',{ name => $funcname });
    # hook
    my $job = Qudo::Model->insert('job',
        {
            func_id => $func_id,
            arg     => $arg,
            uniqkey => $uniqkey,
        }
    );
    # hook
    return $job;
}

sub dequeue {
    my ($self, $job_id) = @_;

    my $job = Qudo::Model->single('job',{id => $job_id});
    return $job;
}

sub can_work {}
sub work {}
sub work_once {}

=head1 NAME

Qudo - Module abstract (<= 44 characters) goes here

=head1 SYNOPSIS

  use Qudo;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for this module was created by ExtUtils::ModuleMaker.
It looks like the author of the extension was negligent enough
to leave the stub unedited.

Blah blah blah.

=head1 AUTHOR

Atsushi Kobayashi <nekokak _at_ gmail dot com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;
