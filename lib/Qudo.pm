package Qudo;
use strict;
use warnings;

our $VERSION = '0.01';

use Qudo::Model;
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

=pod
sub lookup_job {}
sub list_jobs {}
=cut

sub enqueue {
    my ($self, $funcname, $arg, $uniqkey) = @_;

    # hook
    my $func = Qudo::Model->find_or_create('func',{ name => $funcname });
    # hook
    my $job = Qudo::Model->insert('job',
        {
            func_id => $func->id,
            arg     => $arg,
            uniqkey => $uniqkey,
        }
    );

    # hook
    return $job;
}

=pod
sub dequeue {
    my ($self, $job_id) = @_;

    my $job = Qudo::Model->single('job',{id => $job_id});
    return $job;
}

sub can_work {
    my ($self, $funcname) = @_;
    $self->{current_abilities}->{$funcname}=1;
}

sub work {
    my ($self, $delay) = @_;
    $delay ||= 5;
    while (1) {
        sleep $delay unless $self->work_once;
    }
}
=cut

sub work_once {
    my $self = shift;

    my $job = $self->find_job;
    return unless $job;
    my $worker_class = $job->funcname;
    return unless $worker_class;
    $worker_class->work_safely($self, $job);
}

sub find_job {
    my $self = shift;

    my $jobs = Qudo::Model->search('job',{},{limit => $self->{find_job_limit_size}});
    return $self->_grab_a_job($jobs);
}

sub _grab_a_job {
    my ($job, $jobs) = @_;
    $jobs->first;
}

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
