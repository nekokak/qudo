use Qudo::Test;
use Test::More;
use List::Util;

BEGIN {

    @Qudo::Test::SUPPORT_DRIVER = qw/DBI/;

};
my $test_plan = 5;

run_tests($test_plan, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->can_do('Worker::Test');

    my @job_priority = List::Util::shuffle 1..$test_plan;

    for my $priority ( @job_priority ){
        $manager->enqueue(
            "Worker::Test",
            { arg => 'arg'.$priority, priority=>$priority}
        );
    }

    while( my $job = $manager->find_job ){
        is $job->arg , 'arg'.$test_plan;
        $test_plan--;
    }

    teardown_dbs;
});

package Worker::Test;
use base 'Qudo::Worker';
