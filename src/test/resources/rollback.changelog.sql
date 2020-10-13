--liquibase formatted sql changeLogId:52070d27-0e7b-4813-8caa-037f7d44f7fc
--changeset bob:1
CREATE TABLE betterbotz.TESTME2 ( foo int PRIMARY KEY, bar text );
--rollback DROP TABLE betterbotz.TESTME2;
--changeset bob:2
CREATE TABLE betterbotz.TESTME3 ( foo int PRIMARY KEY, bar text );
--rollback DROP TABLE betterbotz.TESTME3;
--changeset bob:3
CREATE TABLE betterbotz.TESTME4 ( foo int PRIMARY KEY, bar text );
--rollback DROP TABLE betterbotz.TESTME4;
--changeset bob:4
CREATE TABLE betterbotz.TESTME5 ( foo int PRIMARY KEY, bar text );
--rollback DROP TABLE betterbotz.TESTME5;