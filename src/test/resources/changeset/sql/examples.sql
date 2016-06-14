--liquibase formatted sql

--changeset examples:0
--preconditions onFail:CONTINUE onError:HALT
--precondition-sql-check expectedResult:1 select count(*) from system.schema_keyspaces where keyspace_name = 'examples';
--comment drop examples keyspace
drop keyspace examples;

--changeset examples:1
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:0 select count(*) from system.schema_keyspaces where keyspace_name = 'examples';
--comment create examples keyspace
create keyspace examples with replication =  { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

--changeset examples:2
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:1 select count(*) from system.schema_keyspaces where keyspace_name = 'examples';
--comment alter examples keyspace
alter keyspace examples with replication =  { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

--changeset examples:3
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:0 select count(*) from system.schema_columnfamilies where keyspace_name = 'examples' and columnfamily_name='test1';
--comment create table example
--rollback drop table test1;
create table examples.test1 (
    id int primary key,
    name text
);

--changeset examples:4
--preconditions onFail:CONTINUE onError:HALT
--precondition-sql-check expectedResult:1 select count(*) from system.schema_columnfamilies where keyspace_name = 'examples' and columnfamily_name='test1';
--comment drop table example
drop table examples.test1;

--changeset examples:5
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:0 select count(*) from system.schema_columnfamilies where keyspace_name = 'examples' and columnfamily_name='test1';
--comment recreate table example
--rollback drop table test1;
create table examples.test1 (
    id int primary key,
    name text
);

--changeset examples:6
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:1 select count(*) from system.schema_columnfamilies where keyspace_name = 'examples' and columnfamily_name='test1';
--comment add column to table
--rollback alter table examples.test1 drop address;
alter table examples.test1 add address text;

--changeset examples:7
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:1 select count(*) from system.schema_columnfamilies where keyspace_name = 'examples' and columnfamily_name='test1';
--comment alter table column
alter table examples.test1 alter address type varchar;

--changeset examples:8
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:1 select count(*) from system.schema_columnfamilies where keyspace_name = 'examples' and columnfamily_name='test1';
--comment drop table column
alter table examples.test1 drop address;

--changeset examples:9
--preconditions onFail:HALT onError:HALT
--precondition-sql-check expectedResult:0 select count(*) from examples.test1;
--comment insert data to a table
--rollback truncate examples.test1;
insert into examples.test1 (id, name) values (1, 'name 1');
insert into examples.test1 (id, name) values (2, 'name 2');

--changeset examples:10
--comment delete all data from a table
truncate examples.test1;





