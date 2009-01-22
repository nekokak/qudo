package Qudo;
use strict;
use warnings;

our $VERSION = '0.01';

use Qudo::Model;
use Qudo::Manager;
use Carp;

our $RETRY_SECONDS = 30;
our $FIND_JOB_LIMIT_SIZE = 30;

sub new {
    my ($class, %args) = @_;

    my $self = bless {}, $class;

    croak "database must be an hashref if specified"
        unless !exists $args{database} || ref $args{database} eq 'HASH';
    my $database = delete $args{database};

    $self->{retry_seconds} = delete $args{retry_seconds} || $RETRY_SECONDS;
    $self->{find_job_limit_size} = delete $args{find_job_limit_size} || $FIND_JOB_LIMIT_SIZE;

    Qudo::Model->connect_info($database);

    return $self;
}

sub manager {
    my $self = shift;
    Qudo::Manager->new(
        master => $self,
    );
}

sub driver { 'Qudo::Model' }

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
