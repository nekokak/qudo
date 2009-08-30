use strict;
use warnings;
use Test::More tests=>3;

use Qudo::Driver::DBI;

my $class = 'Qudo::Driver::DBI';

#_join_func_name
{
    is $class->_join_func_name() , q{ func.name IN () };
    is $class->_join_func_name(['aaa']) , q{ func.name IN (?) };
    is $class->_join_func_name(['aaa','bbb','ccc']) , q{ func.name IN (?,?,?) };
}

1;
