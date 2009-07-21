package Qudo::Worker;
use strict;
use warnings;

sub max_retries { 0 }
sub retry_delay { 0 }
sub grab_for    { 60*60 } # default setting 1 hour

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

=head1 REPOS

http://github.com/nekokak/qudo/tree/master

=head1 AUTHOR

Atsushi Kobayashi <nekokak _at_ gmail dot com>

=head1 COPYRIGHT

This program is free software; you can redistribute
it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the
LICENSE file included with this module.

=cut

1;
