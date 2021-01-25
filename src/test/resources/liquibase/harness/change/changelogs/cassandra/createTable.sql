--liquibase formatted sql
--changeset oleh:1
-- Database: cassandra
CREATE TABLE test_table (test_id INT, test_column VARCHAR, PRIMARY KEY (test_id))
--rollback DROP TABLE test_table;