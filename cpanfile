requires 'DBI';
requires 'DBIx::Skinny';
requires 'Getopt::Long';
requires 'List::Util';
requires 'Pod::Usage';
requires 'Scalar::Util';
requires 'UNIVERSAL::require';
requires 'YAML';

recommends 'JSON::XS';
recommends 'Data::MessagePack';

on configure => sub {
    requires 'Module::Build::Tiny', '0.035';
    requires 'perl', '5.008_001';
};

on test => sub {
    requires 'Test::More';
    requires 'Test::Output';
    requires 'Test::Requires';
    recommends 'Test::Memory::Cycle';
};

on develop => sub {
    requires 'Test::Perl::Critic';
};
