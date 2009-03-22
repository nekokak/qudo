package Qudo::Manager;
use strict;
use warnings;
use Qudo::Job;
use Carp;
use UNIVERSAL::require;

sub new {
    my $class = shift;

    my $self = bless {
        driver              => '',
        find_job_limit_size => '',
        retry_seconds       => '',
        fanc_map            => +{},
        default_hooks       => [],
        hooks               => +{},
        @_
    }, $class;

    $self->register_hooks(@{$self->{default_hooks}});

    return $self;
}

sub driver { shift->{driver} }

sub call_hook {
    my ($self, $hook_point, $args) = @_;

    for my $module (keys %{$self->{hooks}->{$hook_point}}) {
        my $code = $self->{hooks}->{$hook_point}->{$module};
        $code->($args);
    }
}

sub register_hooks {
    my ($self, @hook_modules) = @_;

    for my $module (@hook_modules) {
        $module->require or Carp::croak $@;
        my ($hook_point, $code) = $module->load();
        $self->{hooks}->{$hook_point}->{$module} = $code;
    }
}

sub unregister_hooks {
    my ($self, @hook_modules) = @_;

    for my $module (@hook_modules) {
        my $hook_point = $module->unload();
        delete $self->{hooks}->{$hook_point}->{$module};
    }
}

sub can_do {
    my ($self, $funcname) = @_;
    $self->{func_map}->{$funcname} = 1;
}

sub enqueue {
    my ($self, $funcname, $arg, $uniqkey) = @_;

    my $func_id = $self->driver->get_func_id( $funcname );

    unless ($func_id) {
        croak "$funcname can't get";
    }

    my $args = +{
        func_id => $func_id,
        arg     => $arg,
        uniqkey => $uniqkey,
    };
    $self->call_hook('pre_enqueue', $args);

    my $job_id = $self->driver->enqueue($args);

    return $self->lookup_job($job_id);
}

sub reenqueue {
    my ($self, $job, $args) = @_;

    $self->driver->reenqueue($job->id, $args);

    return $self->lookup_job($job->id);
}

sub dequeue {
    my ($self, $job) = @_;
    $self->driver->dequeue({id => $job->id});
}

sub work_once {
    my $self = shift;

    my $job = $self->find_job;
    return unless $job;

    my $worker_class = $job->funcname;
    return unless $worker_class;

    $self->call_hook('pre_work', $job);

    $worker_class->work_safely($self, $job);

    $self->call_hook('post_work', $job);
}

sub lookup_job {
    my ($self, $job_id) = @_;

    my $callback = $self->driver->lookup_job($job_id);

    return $self->_grab_a_job($callback);
}

sub find_job {
    my $self = shift;

    return unless keys %{$self->{func_map}};
    my $callback = $self->driver->find_job($self->{find_job_limit_size}, $self->{func_map});

    return $self->_grab_a_job($callback);
}

sub _grab_a_job {
    my ($self, $callback) = @_;

    while (1) {
        my $job_data = $callback->();
        last unless $job_data;

        my $old_grabbed_until = $job_data->{job_grabbed_until};
        my $server_time = $self->driver->get_server_time
            or die "expected a server time";

        my $worker_class = $job_data->{func_name};
        my $grab_job = $self->driver->grab_a_job(
            grabbed_until     => ($server_time + ($worker_class->grab_for || 1)),
            job_id            => $job_data->{job_id},
            old_grabbed_until => $old_grabbed_until,
        );
        next unless $grab_job;

        my $job = Qudo::Job->new(
            manager  => $self,
            job_data => $job_data,
        );
        return $job;
    }
    return;
}

sub job_failed {
    my ($self, $job, $message) = @_;

    $self->driver->logging_exception(
        {
            job_id  => $job->id,
            func_id => $job->func_id,
            message => $message,
        }
    );
}

1;

