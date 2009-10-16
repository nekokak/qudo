package Qudo::Driver::Skinny::Schema;
use DBIx::Skinny::Schema;

install_table job => schema {
    pk 'id';
    columns qw/id func_id arg uniqkey enqueue_time grabbed_until run_after retry_cnt priority/;

    trigger pre_insert => callback {
        my ($class, $args) = @_;
        $args->{enqueue_time}  ||= time;
        $args->{grabbed_until} ||= 0;
        $args->{retry_cnt}     ||= 0;
        $args->{priority}      ||= 0;
        $args->{run_after}     = time + ($args->{run_after}||0);
    };

    trigger pre_update => callback {
        my ($class, $args) = @_;
        $args->{enqueue_time} = time;
        $args->{run_after}    = time + (delete $args->{retry_delay}||0);
    };
};

install_table func => schema {
    pk 'id';
    columns qw/id name/;
};

install_table exception_log => schema {
    pk 'id';
    columns qw/id func_id message arg exception_time retried/;

    trigger pre_insert => callback {
        my ($class, $args) = @_;
        $args->{exception_time} ||= time;
        $args->{retried} = 0;
    };
};

install_table job_status => schema {
    pk 'id';
    columns qw/id func_id arg uniqkey status job_start_time job_end_time/;
};

1;

