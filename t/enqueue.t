use strict;
use warnings;
use Qudo::Test;
use Test::More tests => 6;

run_tests(3, sub {
    my $master = test_client(
        dbname   => 'tq1',
    );

    my $job = $master->manager->enqueue("Worker::Test", 'arg', 'uniqkey');

    is $job->id, 1;
    is $job->arg, 'arg';
    is $job->uniqkey, 'uniqkey';

    teardown_db('tq1');
});

