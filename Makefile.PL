use inc::Module::Install;
name 'Qudo';
all_from 'lib/Qudo.pm';

requires 'DBIx::Skinny';
requires 'Carp';
requires 'UNIVERSAL::require';
requires 'DBI';
requires 'YAML';
requires 'Class::Data::Inheritable';

recommends 'JSON::XS';
recommends 'Data::MessagePack';

build_requires 'Test::More';
build_requires 'Test::Output';
build_requires 'Test::Requires';
auto_include;

install_script 'bin/qudo';

tests 't/*.t t/*/*.t t/*/*/*.t';
author_tests('xt');

auto_include;
auto_set_repository;

WriteAll;

