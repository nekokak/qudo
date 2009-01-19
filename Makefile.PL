use inc::Module::Install;
name 'Qudo';
all_from 'lib/Qudo.pm';

build_requires 'Test::More';
use_test_base;
auto_include;
WriteAll;
