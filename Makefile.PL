use inc::Module::Install;
name 'Qudo';
all_from 'lib/Qudo.pm';

requires 'DBIx::Skinny';
requires 'Carp';
requires 'UNIVERSAL::require';
requires 'DBI';
requires 'YAML';
requires 'Class::Data::Inheritable';

build_requires 'Test::More';
build_requires 'Test::Output';
auto_include;

install_share 'doc';

use_test_base;
tests 't/*.t t/*/*.t t/*/*/*.t';
author_tests('xt');

auto_include;
auto_set_repository;

WriteAll;

