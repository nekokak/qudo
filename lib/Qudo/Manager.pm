package Qudo::Manager;
use strict;
use warnings;
use Qudo::Job;
use Carp;
use UNIVERSAL::require;
use Qudo::HookLoader;

sub new {
    my $class = shift;

    my $self = bless {
        driver              => '',
        find_job_limit_size => '',
        retry_seconds       => '',
        fanc_map            => +{},
        default_hooks       => [],
        default_plugins     => [],
        hooks               => +{},
        plugin              => +{},
        abilities           => [],
        @_
    }, $class;

    $self->global_register_hooks(@{$self->{default_hooks}});
    $self->register_plugins(@{$self->{default_plugins}});
    $self->register_abilities(@{$self->{abilities}});

    return $self;
}

sub driver { $_[0]->{driver} }
sub plugin { $_[0]->{plugin} }

sub register_abilities {
    my ($self, @abilities) = @_;

    for my $ability (@abilities) {
        $self->can_do($ability);
    }
}

sub has_abilities {
    keys %{$_[0]->{func_map}};
}

sub register_plugins {
    my ($self, @plugins) = @_;

    for my $plugin (@plugins) {
        $plugin->require or Carp::croak $@;
        my ($plugin_name, $code) = $plugin->load();
        $self->{plugin}->{$plugin_name} = $code;
    }
}

sub call_hook {
    my ($self, $hook_point, $worker_class, $args) = @_;

    for my $module (keys %{$worker_class->hooks->{$hook_point}}) {
        my $code = $worker_class->hooks->{$hook_point}->{$module};
        $code->($args);
    }

    for my $module (keys %{$self->hooks->{$hook_point}}) {
        my $code = $self->hooks->{$hook_point}->{$module};
        $code->($args);
    }
}

sub hooks { $_[0]->{hooks} }

sub global_register_hooks {
    my ($self, @hook_modules) = @_;

    Qudo::HookLoader->register_hooks($self, \@hook_modules);
}

sub global_unregister_hooks {
    my ($self, @hook_modules) = @_;

    Qudo::HookLoader->unregister_hooks($self, \@hook_modules);
}

sub can_do {
    my ($self, $funcname) = @_;

    $funcname->use;
    $self->{func_map}->{$funcname} = 1;
}

sub enqueue {
    my ($self, $funcname, $arg) = @_;

    my $func_id = $self->{_func_id_cache}->{$funcname} ||= $self->driver->get_func_id( $funcname );

    unless ($func_id) {
        croak "$funcname can't get";
    }

    my $args = +{
        func_id   => $func_id,
        arg       => $arg->{arg},
        uniqkey   => $arg->{uniqkey},
        run_after => $arg->{run_after}||0,
    };

    $self->call_hook('pre_enqueue', $funcname, $args);
    $self->call_hook('serialize',   $funcname, $args);

    my $job_id = $self->driver->enqueue($args);
    my $job = $self->lookup_job($job_id);

    $self->call_hook('post_enqueue', $funcname, $job);

    return $job;
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

    $self->call_hook('deserialize', $worker_class, $job);
    $self->call_hook('pre_work',    $worker_class, $job);

    my $res = $worker_class->work_safely($self, $job);

    $self->call_hook('post_work', $worker_class, $job);

    return $res;
}

sub lookup_job {
    my ($self, $job_id) = @_;

    my $callback = $self->driver->lookup_job($job_id);
    my $job_data = $callback->();
    return $job_data ? $self->_data2job($job_data) : undef;
}

sub find_job {
    my $self = shift;

    return unless keys %{$self->{func_map}};
    my $callback = $self->driver->find_job($self->{find_job_limit_size}, $self->{func_map});

    return $self->_grab_a_job($callback);
}

sub _data2job {
    my ($self, $job_data) = @_;

    Qudo::Job->new(
        manager  => $self,
        job_data => $job_data,
    );
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
            grabbed_until     => ($server_time + $worker_class->grab_for),
            job_id            => $job_data->{job_id},
            old_grabbed_until => $old_grabbed_until,
        );
        next if $grab_job < 1;

        return $self->_data2job($job_data);
    }
    return;
}

sub job_failed {
    my ($self, $job, $message) = @_;

    $self->driver->logging_exception(
        {
            func_id => $job->func_id,
            message => $message,
            uniqkey => $job->uniqkey,
            arg     => $job->arg_origin || $job->arg,
        }
    );
}

sub enqueue_from_failed_job {
    my ($self, $exception_log) = @_;

    if ( $exception_log->{retried_fg} ) {
        Carp::carp('this exception is already retried');
        return;
    }
    my $args = +{
        func_id => $exception_log->{func_id},
        arg     => $exception_log->{arg},
        uniqkey => $exception_log->{uniqkey},
    };

    my $job_id = $self->driver->enqueue($args);

    $self->driver->retry_from_exception_log($exception_log->{id});

    $self->lookup_job($job_id);
}

1;

