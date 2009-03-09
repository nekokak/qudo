use strict;
use warnings;
use Qudo::Test;
use Test::More;

run_tests(3, sub {
    my $master = test_master(
        dbname   => 'tq1',
        driver   => 'Skinny',
    );

    my $job = $master->manager->enqueue("Worker::Test", 'arg', 'uniqkey');

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';
