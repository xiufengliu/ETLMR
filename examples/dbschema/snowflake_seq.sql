begin;

-- schema
drop schema if exists etlmr cascade;
create schema etlmr;
set search_path to etlmr;


-- tables
create table topleveldomaindim(
   topleveldomainid SERIAL,
   topleveldomain varchar
);

create table domaindim(
   domainid SERIAL,
   domain varchar,
   topleveldomainid int
);


create table serverdim(
   serverid SERIAL,
   server varchar
);

create table serverversiondim(
   serverversionid SERIAL,
   serverversion varchar,
   serverid int
);

create table pagedim(
   pageid SERIAL,
   url varchar,
   size int,
   validfrom date,
   validto date,
   version int,
   domainid int,
   serverversionid int
);

create table testdim(
   testid int,
   testname varchar,
   testauthor varchar
);
insert into testdim values (0, 'Test0', 'Joe Doe'), (1, 'Test1', 'Joe Doe'), 
(2, 'Test2', 'Joe Doe'), (3, 'Test3', 'Joe Doe'), (4, 'Test4', 'Joe Doe'),
(5, 'Test5', 'Joe Doe'), (6, 'Test6', 'Joe Doe'), (7, 'Test7', 'Joe Doe'),
(8, 'Test8', 'Joe Doe'), (9, 'Test9', 'Joe Doe'),
(-1, 'Unknown test', 'Unknown author');

create table datedim(
   dateid SERIAL,
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
--alter table topleveldomaindim add primary key(topleveldomainid);
--alter table domaindim add primary key(domainid);
--alter table serverdim add primary key(serverid);
--alter table serverversiondim add primary key(serverversionid);
--alter table pagedim add primary key(pageid);
--alter table testdim add primary key(testid);
--alter table datedim add primary key(dateid);
--alter table testresultsfact add primary key(pageid, testid, dateid);

-- foreign keys
-- alter table domaindim add foreign key(topleveldomainid) references topleveldomaindim;
-- alter table serverversiondim add foreign key(serverid) references serverdim;
-- alter table pagedim add foreign key(domainid) references domaindim, 
--   add foreign key(serverversionid) references serverversiondim;
-- alter table testresultsfact add foreign key(pageid) references pagedim,
--   add foreign key(testid) references testdim,
--   add foreign key(dateid) references datedim;


-- indexes

create index url_version_idx on pagedim(url, version desc);

commit;
