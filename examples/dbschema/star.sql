begin;

-- schema
drop schema if exists etlmr cascade;
create schema etlmr;
set search_path to etlmr;


-- tables
create table pagedim(
   pageid int,
   url varchar,
   size int,
   validfrom date,
   validto date,
   version int,
   domain varchar,
   serverversion varchar
);

create table testdim(
   testid int,
   testname varchar,
   testauthor varchar
);

--insert into testdim values (0, 'Test0', 'Joe Doe'), (1, 'Test1', 'Joe Doe'), 
--(2, 'Test2', 'Joe Doe'), (3, 'Test3', 'Joe Doe'), (4, 'Test4', 'Joe Doe'),
--(5, 'Test5', 'Joe Doe'), (6, 'Test6', 'Joe Doe'), (7, 'Test7', 'Joe Doe'),
--(8, 'Test8', 'Joe Doe'), (9, 'Test9', 'Joe Doe'),
--(-1, 'Unknown test', 'Unknown author');

create table datedim(
   dateid int,
   date date,
   day int,
   month int,
   year int,
   week int,
   weekyear int
);

create table testresultsfact(
   pageid int,
   testid int,
   dateid int,
   errors int
);


-- primary keys
alter table pagedim add primary key(pageid);
alter table testdim add primary key(testid);
alter table datedim add primary key(dateid);
alter table testresultsfact add primary key(pageid, testid, dateid);


-- Create sequence
CREATE  SEQUENCE seq_pageid INCREMENT  BY 10000 NO MAXVALUE START 1;

-- indexes
--create index url_version_idx on pagedim(url, version desc);

commit;
