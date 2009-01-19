package Qudo::Model::Schema;
use DBIx::Skinny::Schema;

install_table job => schema {
    pk 'id';
    columns qw/id func_id arg uniqkey enqueue_time/;

    trigger pre_insert => callback {
        my ($class, $args) = @_;
        $args->{enqueue_time} = time;
    };
};

install_table func => schema {
    pk 'id';
    columns qw/id name/;
};

install_table exception_log => schema {
    pk 'id';
    columns qw/id func_id job_id messages exception_time/;

    trigger pre_insert => callback {
        my ($class, $args) = @_;
        $args->{exception_time} = time;
    };
};

=pod
install_inflate_rule '^.+_time$' => callback {
    inflate {
        my $value = shift;
        my $dt = DateTime::Format::Strptime->new(
            pattern   => '%Y-%m-%d %H:%M:%S',
            time_zone => $timezone,
        )->parse_datetime($value);
        return DateTime->from_object( object => $dt );
    };
    deflate {
        my $value = shift;
        return DateTime::Format::MySQL->format_datetime($value);
    };
};
=cut

1;

