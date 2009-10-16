use strict;
use warnings;
use Qudo::Test;
use Test::More;
use lib './t';

BEGIN {
  eval "use Data::MessagePack";
  plan skip_all => 'needs Data::MessagePack for testing' if $@;
}

my %hash = ( key => 'arg' );
run_tests(12, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');
    $manager->can_do('Worker::Test2');

    { # load Qudo::Hook::Serialize::MessagePack
        $manager->global_register_hooks(qw/Qudo::Hook::Serialize::MessagePack/);

        my $job = $manager->enqueue("Worker::Test", { arg => \%hash , uniqkey => 'uniqkey1'});

        is $job->id, 1;
        is $job->arg, Data::MessagePack->pack( \%hash );
        is $job->uniqkey, 'uniqkey1';

        sleep(1);

        my $res = $manager->work_once;
        is_deeply $res, \%hash ;

    }

    { # failed worker by Qudo::Hook::Serialize::MessagePack
        my $job = $manager->enqueue("Worker::Test2", { arg => \%hash , uniqkey => 'uniqkey1'});

        is $job->id, 2;
        is $job->arg, Data::MessagePack->pack( \%hash );
        is $job->uniqkey, 'uniqkey1';

        sleep(1);

        $manager->work_once; # worker failed
        my $exception = $master->exception_list;
        is $exception->[0]->{arg} , Data::MessagePack->pack( \%hash );
    }

    { # unload Qudo::Hook::Serialize::MessagePack
        $manager->global_unregister_hooks(qw/Qudo::Hook::Serialize::MessagePack/);

        my $job = $master->manager->enqueue("Worker::Test", { arg => 'arg' , uniqkey => 'uniqkey2'});

        is $job->id, 3;
        is $job->arg , 'arg';
        is $job->uniqkey, 'uniqkey2';

        sleep(1);

        my $res = $manager->work_once;
        is $res , 'arg';
    }

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    my ($class, $job) = @_;
    $job->completed;
    return $job->arg;
}

package Worker::Test2;
use base 'Qudo::Worker';

sub grab_for { 0 }
sub work {
    my ($class, $job) = @_;
    die 'failed worker';
}
