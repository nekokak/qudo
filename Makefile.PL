use inc::Module::Install;
name 'Qudo';
all_from 'lib/Qudo.pm';

build_requires 'Test::More';
auto_include;

use_test_base;
tests 't/*.t t/*/*.t t/*/*/*.t';
author_tests 'xt';
WriteAll;

