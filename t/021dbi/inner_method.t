use strict;
use warnings;
use Test::More tests=>5;

use Qudo::Driver::DBI;

my $class = 'Qudo::Driver::DBI';

# _join_func_name
{
    is $class->_join_func_name() , q{ func.name IN () };
    is $class->_join_func_name(['aaa']) , q{ func.name IN (?) };
    is $class->_join_func_name(['aaa','bbb','ccc']) , q{ func.name IN (?,?,?) };
}

# _build_insert_sql
{
     my $table  = 'test_table';
     my @columm = ( qw/hoge/);
     my $ret = $class->_build_insert_sql( $table , \@columm );
     is $ret , qq{ INSERT INTO $table ( hoge ) VALUES ( ? )};

     $table .= '_2';
     map { push @columm , $_ }  qw/ moge fuga / ;
     $ret = $class->_build_insert_sql( $table , \@columm );
     is $ret , qq{ INSERT INTO $table ( hoge , moge , fuga ) VALUES ( ? , ? , ? )};
}

1;
