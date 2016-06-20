--liquibase formatted sql

--changeset examples:1
--preconditions onFail:HALT onError:HALT
--comment create examples keyspace
create keyspace if not exists examples with replication =  { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

--changeset examples:2
--preconditions onFail:HALT onError:HALT
--comment alter examples keyspace
alter keyspace examples with replication =  { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

--changeset examples:3
--preconditions onFail:HALT onError:HALT
--comment create table example
--rollback drop table test1;
create table if not exists  examples.test1 (
    id int primary key,
    name text
);

--changeset examples:4
--preconditions onFail:CONTINUE onError:HALT
--comment drop table example
drop table examples.test1;

--changeset examples:5
--preconditions onFail:HALT onError:HALT
--comment recreate table example
--rollback drop table test1;
create table examples.test1 (
    id int primary key,
    name text
);

--changeset examples:6
--preconditions onFail:HALT onError:HALT
--comment add column to table
--rollback alter table examples.test1 drop address;
alter table examples.test1 add address text;

--changeset examples:7
--preconditions onFail:HALT onError:HALT
--comment alter table column
alter table examples.test1 alter address type varchar;

--changeset examples:8
--preconditions onFail:HALT onError:HALT
--comment drop table column
alter table examples.test1 drop address;

--changeset examples:9
--preconditions onFail:HALT onError:HALT
--comment insert data to a table
--rollback truncate examples.test1;
insert into examples.test1 (id, name) values (1, 'name 1');
insert into examples.test1 (id, name) values (2, 'name 2');

--changeset examples:10
--comment delete all data from a table
truncate examples.test1;


