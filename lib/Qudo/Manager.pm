package Qudo::Manager;
use strict;
use warnings;
use Qudo::Job;
use Carp;
use UNIVERSAL::require;
use Qudo::HookLoader;
use Scalar::Util qw/weaken/;

sub new {
    my $class = shift;

    my $self = bless {
        driver_for          => '',
        shuffled_databases  => '',
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
    weaken($self->{qudo});

    $self->global_register_hooks(@{$self->{default_hooks}});
    $self->register_plugins(@{$self->{default_plugins}});
    $self->register_abilities(@{$self->{abilities}});

    return $self;
}

sub driver_for { $_[0]->{qudo}->driver_for($_[1]) }
sub shuffled_databases { $_[0]->{qudo}->shuffled_databases() }
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

sub funcname_to_id {
    my ($self, $funcname, $db) = @_;
    $self->{_func_cache}->{$db}->{funcname2id}->{$funcname} ||= $self->driver_for($db)->get_func_id( $funcname );
}

sub funcid_to_name {
    my ($self, $funcid, $db) = @_;
    $self->{_func_cache}->{$db}->{funcid2name}->{$funcid} ||= $self->driver_for($db)->get_func_name( $funcid );
}

sub enqueue {
    my ($self, $funcname, $arg) = @_;

    my $db = $self->shuffled_databases;
    my $func_id = $self->funcname_to_id($funcname, $db);

    unless ($func_id) {
        croak "$funcname can't get";
    }

    my $args = +{
        func_id   => $func_id,
        arg       => $arg->{arg},
        uniqkey   => $arg->{uniqkey},
        run_after => $arg->{run_after}||0,
        priority  => $arg->{priority} ||0,
    };

    $self->call_hook('pre_enqueue', $funcname, $args);
    $self->call_hook('serialize',   $funcname, $args);

    my $job_id = $self->driver_for($db)->enqueue($args);
    my $job = $self->lookup_job($job_id, $db);

    $self->call_hook('post_enqueue', $funcname, $job);

    return $job;
}

sub reenqueue {
    my ($self, $job, $args) = @_;

    my $db = $self->shuffled_databases;
    $self->driver_for($db)->reenqueue($job->id, $args);

    return $self->lookup_job($job->id);
}

sub dequeue {
    my ($self, $job) = @_;
    $self->driver_for($job->db)->dequeue({id => $job->id});
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
    my ($self, $job_id, $db) = @_;

    $db ||= $self->shuffled_databases;

    my $callback = $self->driver_for($db)->lookup_job($job_id);
    my $job_data = $callback->();
    return $job_data ? $self->_data2job($job_data, $db) : undef;
}

sub find_job {
    my $self = shift;

    for my $db ($self->shuffled_databases) {
        return unless keys %{$self->{func_map}};
        my $callback = $self->driver_for($db)->find_job($self->{find_job_limit_size}, $self->{func_map});

        return $self->_grab_a_job($callback, $db);
    }
}

sub _data2job {
    my ($self, $job_data, $db) = @_;

    Qudo::Job->new(
        manager  => $self,
        job_data => $job_data,
        db       => $db,
    );
}

sub _grab_a_job {
    my ($self, $callback, $db) = @_;

    while (1) {
        my $job_data = $callback->();
        last unless $job_data;

        my $old_grabbed_until = $job_data->{job_grabbed_until};
        my $server_time = $self->driver_for($db)->get_server_time
            or die "expected a server time";

        my $worker_class = $job_data->{func_name};
        my $grab_job = $self->driver_for($db)->grab_a_job(
            grabbed_until     => ($server_time + $worker_class->grab_for),
            job_id            => $job_data->{job_id},
            old_grabbed_until => $old_grabbed_until,
        );
        next if $grab_job < 1;

        return $self->_data2job($job_data, $db);
    }
    return;
}

sub job_failed {
    my ($self, $job, $message) = @_;

    $self->driver_for($job->db)->logging_exception(
        {
            func_id => $job->func_id,
            message => $message,
            uniqkey => $job->uniqkey,
            arg     => $job->arg_origin || $job->arg,
        }
    );
}

sub set_job_status {
    my ($self, $job, $status) = @_;

    $self->driver_for($job->db)->set_job_status(
        {
            func_id        => $job->func_id,
            arg            => $job->arg_origin || $job->arg,
            uniqkey        => $job->uniqkey,
            status         => $status,
            job_start_time => $job->job_start_time,
            job_end_time   => time(),
        }
    );
}

sub enqueue_from_failed_job {
    my ($self, $exception_log, $db) = @_;

    if ( $exception_log->{retried} ) {
        Carp::carp('this exception is already retried');
        return;
    }
    my $args = +{
        func_id => $exception_log->{func_id},
        arg     => $exception_log->{arg},
        uniqkey => $exception_log->{uniqkey},
    };

    my $job_id = $self->driver_for($db)->enqueue($args);

    $self->driver_for($db)->retry_from_exception_log($exception_log->{id});

    $self->lookup_job($job_id, $db);
}

1;

