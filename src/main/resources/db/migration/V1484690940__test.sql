-- CLASSIFICATION NOTICE: This file is UNCLASSIFIED

create table test (
    id          integer identity          primary  key,
    abbrev      varchar(80),
    short_name  varchar(256),
    long_name   varchar(256)
);

insert into test (abbrev, short_name, long_name) values ('AA', 'America', 'United States of America');
insert into test (abbrev, short_name, long_name) values ('GB', 'Britian', 'Great Britain');
insert into test (abbrev, short_name, long_name) values ('Fr', 'France',  'France');

