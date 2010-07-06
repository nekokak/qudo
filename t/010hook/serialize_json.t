use Qudo::Test;
use Test::More;

BEGIN {
  eval "use JSON::XS";
  plan skip_all => 'needs JSON::XS for testing' if $@;
}

my $RESULT;
run_tests(12, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->can_do('Worker::Test2');

    { # load Qudo::Hook::Serialize::JSON
        $manager->register_hooks(qw/Qudo::Hook::Serialize::JSON/);

        my $job = $manager->enqueue("Worker::Test", { arg => {key => 'arg'}, uniqkey => 'uniqkey1'});

        is $job->id, 1;
        is $job->arg, '{"key":"arg"}';
        is $job->uniqkey, 'uniqkey1';

        sleep(1);

        $manager->work_once;
        is_deeply $RESULT, {key => 'arg'};

    }

    { # failed worker by Qudo::Hook::Serialize::JSON
        my $job = $manager->enqueue("Worker::Test2", { arg => {key => 'arg'}, uniqkey => 'uniqkey1'});

        is $job->id, 2;
        is $job->arg, '{"key":"arg"}';
        is $job->uniqkey, 'uniqkey1';

        sleep(1);

        $manager->work_once; # worker failed
        my $exception = $master->exception_list;
        my ($db, $rows) = each %$exception;
        is $rows->[0]->{arg}, '{"key":"arg"}';
    }

    { # unload Qudo::Hook::Serialize::JSON
        $manager->unregister_hooks(qw/Qudo::Hook::Serialize::JSON/);

        my $job = $master->manager->enqueue("Worker::Test", { arg => 'arg', uniqkey => 'uniqkey2'});

        is $job->id, 3;
        is $job->arg, 'arg';
        is $job->uniqkey, 'uniqkey2';

        sleep(1);

        $manager->work_once;
        is $RESULT, 'arg';
    }

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
    $RESULT = $job->arg;
}

package Worker::Test2;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    my ($class, $job) = @_;
    die 'failed worker';
}
