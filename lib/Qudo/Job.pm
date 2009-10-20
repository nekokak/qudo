package Qudo::Job;
use strict;
use warnings;

sub new {
    my ($class, %args) = @_;
    bless {%args}, $class;
}

sub id       { $_[0]->{job_data}->{job_id}       }
sub uniqkey  { $_[0]->{job_data}->{job_uniqkey}  }
sub func_id  { $_[0]->{job_data}->{func_id}      }
sub funcname { $_[0]->{job_data}->{func_name}    }
sub retry_cnt     { $_[0]->{job_data}->{job_retry_cnt}     }
sub grabbed_until { $_[0]->{job_data}->{job_grabbed_until} }
sub priority { $_[0]->{job_data}->{job_priority} }
sub arg : lvalue  { $_[0]->{job_data}->{job_arg}           }
sub arg_origin : lvalue { $_[0]->{arg_origin} }
sub db { $_[0]->{db} }

sub manager  { $_[0]->{manager} }
sub job_start_time : lvalue { $_[0]->{job_start_time} }

sub completed {
    my $self = shift;
    $self->{_complete} = 1;
    return unless $self->funcname->set_job_status;
    $self->manager->set_job_status($self, 'completed');
}

sub is_completed { $_[0]->{_complete} }

sub reenqueue {
    my ($self, $args) = @_;
    $self->manager->reenqueue($self, $args);
}

1;

