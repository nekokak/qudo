package Qudo::Worker;
use strict;
use warnings;
use base 'Class::Data::Inheritable';
use Qudo::HookLoader;

__PACKAGE__->mk_classdata(qw/_hooks/);

sub max_retries { 0 }
sub retry_delay { 0 }
sub grab_for    { 60*60 } # default setting 1 hour
sub hooks {
    my $class = shift;
    $class->_hooks(+{}) unless $class->_hooks;
    $class->_hooks;
}

sub register_hooks {
    my ($class, @hook_modules) = @_;
    Qudo::HookLoader->register_hooks($class, \@hook_modules);
}

sub unregister_hooks {
    my ($class, @hook_modules) = @_;
    Qudo::HookLoader->unregister_hooks($class, \@hook_modules);
}

sub work_safely {
    my ($class, $manager, $job) = @_;
    my $res;

    eval {
        $res = $class->work($job);
    };

    if ( my $e = $@ || ! $job->is_completed ) {
        if ( $job->retry_cnt < $class->max_retries ) {
            $job->reenqueue(
                {
                    retry_cnt   => $job->retry_cnt + 1,
                    retry_delay => $class->retry_delay,
                }
            );
        } else {
            $manager->dequeue($job);
        }
        $manager->job_failed($job, $e || 'Job did not explicitly complete or fail');
    } else {
        $manager->dequeue($job);
    }

    return $res;
}

=head1 NAME

Qudo::Worker - superclass for defining task behavior of Qudo's work

=head1 SYNOPSIS

    package Myworker;
    use base qw/ Qudo::Worker /;

    sub work {
        my ($self , $job ) = @_;

        my $job_arg = $job->arg();
        print "This is Myworker's work. job has argument == $job_arg \n";

        $job->completed();
    }
    ### end of Myworker package.

=head1 DESCRIPTION

Qudo::Worker is based on all your work class of using Qudo.

Your application have to inherit Qudo::Worker anyway.
And it has to have 'work' method too.

'work' method accept Qudo::Job object at parameter.
If your work complete , you may call Qudo::Job->complete() method.

=cut

1;
