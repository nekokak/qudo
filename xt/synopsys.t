use strict;
use warnings;
use Test::More;
eval "use Test::Synopsis";
plan skip_all => 'Test::Synopsis required for testisg' if $@;

all_synopsis_ok();
