package Qudo::Job;
use strict;
use warnings;

sub new {
    my ($class, %args) = @_;
    bless {%args}, $class;
}

sub id       { shift->{job_data}->{job_id}       }
sub arg      { shift->{job_data}->{job_arg}      }
sub uniqkey  { shift->{job_data}->{job_uniqkey}  }
sub func_id  { shift->{job_data}->{func_id}      }
sub funcname { shift->{job_data}->{func_name}    }
sub retry_cnt     { shift->{job_data}->{job_retry_cnt}     }
sub grabbed_until { shift->{job_data}->{job_grabbed_until} }

sub completed {
    my $self = shift;
    $self->{_complete} = 1;
    $self->{manager}->dequeue_job($self);
}

sub is_completed {
    my $self = shift;
    $self->{_complete};
}

sub reenqueue {
    my ($self, $args) = @_;
    $self->{manager}->reenqueue($self, $args);
}

1;

