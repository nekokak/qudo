use strict;
use warnings;
use Qudo::Test;
use Qudo::Model;
use Test::More tests => 6;
use Test::Output;

run_tests(3, sub {
    my $client = test_client(
        dbname   => 'tq1',
    );

    my $job = $client->enqueue("Worker::Test", 'arg', 'uniqkey');
    $client->work_once;
    my $exception = Qudo::Model->single('exception_log');
    like $exception->message, qr/failed worker/;
    is $exception->func_id, 1;
    is $exception->job_id, 1;

    teardown_db('tq1');
});

package Worker::Test;
use base 'Qudo::Worker';

sub work {
    my ($class, $job) = @_;
    die "failed worker";
}
