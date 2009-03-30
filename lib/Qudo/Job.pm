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
sub arg : lvalue  { $_[0]->{job_data}->{job_arg}           }

sub manager  { $_[0]->{manager} }

sub completed { $_[0]->{_complete} = 1 }

sub is_completed { $_[0]->{_complete} }

sub reenqueue {
    my ($self, $args) = @_;
    $self->manager->reenqueue($self, $args);
}

1;

