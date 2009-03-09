use strict;
use warnings;
use Qudo::Test;
use Test::Output;
use lib './t';

run_tests(1, sub {
    my $master = test_master(
        dbname => 'tq1',
        driver   => 'Skinny',
    );

    $master->register_hook(qw/Mock::Hook::PostWork/);

    my $manager = $master->manager;
    $manager->enqueue("Worker::Test", 'arg', 'uniqkey1');
    stdout_is( sub { $manager->work_once } , "Worker::Test: post worked!\n");

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';
