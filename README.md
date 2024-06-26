liquibase-cassandra [![Build and Test Extension](https://github.com/liquibase/liquibase-cassandra/actions/workflows/test.yml/badge.svg)](https://github.com/liquibase/liquibase-cassandra/actions/workflows/test.yml)
===================

Liquibase extension for Cassandra Support.

# Using the Liquibase Test Harness in the Cassandra Extension
The liquibase-cassandra extension now comes with integration test support via the liquibase-test-harness.
This Liquibase test framework is designed to *also* make it easy for you to test your extensions.

### Configuring your project

#### Configuring your connections

We have provided a `harness-config.yml` file in `src/test/resources` directory.
This file should contain the connection information for all the databases you want the Cassandra extension to be tested against.


#### Configuring test DB
The test-harness requires the database under test to be created beforehand, so that the tests might have some data to manipulate, are independent of each other and don't rely on any specific run order.

To be able to run the harness tests locally setting up docker container is currently the simplest route. But if you already have a test instance running elsewhere, use the `test.cql` script to populate it with test data.

To create a local test database docker container, execute the following steps:
- Run main cassandra instance. It could be Cassandra official image or VMware's `bitnami` image. Second one allows to provide password during startup, and we use it in our CI/CD.
Official cassandra image doesn't care about auth. Full list of possible versions for official image can be found in [dockerhub Cassandra official](https://hub.docker.com/_/cassandra) page,
  for `bitnami` go to [VMware's bitnami dockerhub](https://hub.docker.com/r/bitnami/cassandra) page. So commands could be
  - `docker run --name cassandra -e CASSANDRA_PASSWORD=Password1 -e CASSANDRA_PASSWORD_SEEDER=yes -p 9042:9042 -d bitnami/cassandra` or
  - `docker run -p 9042:9042 --rm --name cassandra3 -d cassandra:3` 
 Give container a minute to fully initialize.
  
- run 
  - `docker inspect cassandra` or
  - `docker inspect cassandra3` depending which container you started to get a main instance IP address. By default, it's 172.17.0.2 but may change in our local env.
- to execute init script run second container 
  - `docker run -it --rm cassandra bash` or 
  - `docker run -it --rm bitnami/cassandra bash`, they are interchangeable in our case
- enter cql console
  - `cqlsh 172.17.0.2` or
  - `cqlsh 172.17.0.2 -u cassandra -p Password1` for `bitnami`'s image (or use other IP showed by `docker inspect` if this doesn't work)
- copy and paste init script from `test.cql` file content to create keyspace and tables for tests. Also provided here for your convenience
```
CREATE KEYSPACE betterbotz
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };
USE betterbotz;
DROP TABLE IF EXISTS authors;
CREATE TABLE authors (
                         id int,
                         first_name varchar,
                         last_name varchar,
                         email varchar,
                         birthdate date,
                         added timestamp,
                         PRIMARY KEY (id)
);

INSERT INTO authors(id, first_name, last_name, email, birthdate, added) VALUES
(1,'Eileen','Lubowitz','ppaucek@example.org','1991-03-04','2004-05-30 02:08:25');
INSERT INTO authors(id, first_name, last_name, email, birthdate, added) VALUES
(2,'Tamia','Mayert','shansen@example.org','2016-03-27','2014-03-21 02:52:00');
INSERT INTO authors(id, first_name, last_name, email, birthdate, added) VALUES
(3,'Cyril','Funk','reynolds.godfrey@example.com','1988-04-21','2011-06-24 18:17:48');
INSERT INTO authors(id, first_name, last_name, email, birthdate, added) VALUES
(4,'Nicolas','Buckridge','xhoeger@example.net','2017-02-03','2019-04-22 02:04:41');
INSERT INTO authors(id, first_name, last_name, email, birthdate, added) VALUES
(5,'Jayden','Walter','lillian66@example.com','2010-02-27','1990-02-04 02:32:00');


DROP TABLE IF EXISTS posts;
CREATE TABLE posts (
                       id int,
                       author_id int,
                       title varchar,
                       description varchar,
                       content text,
                       inserted_date date,
                       PRIMARY KEY (id)
);

INSERT INTO posts(id, author_id, title, description, content, inserted_date) VALUES
(1,1,'temporibus','voluptatum','Fugit non et doloribus repudiandae.','2015-11-18');
INSERT INTO posts(id, author_id, title, description, content, inserted_date) VALUES
(2,2,'ea','aut','Tempora molestias maiores provident molestiae sint possimus quasi.','1975-06-08');
INSERT INTO posts(id, author_id, title, description, content, inserted_date) VALUES
(3,3,'illum','rerum','Delectus recusandae sit officiis dolor.','1975-02-25');
INSERT INTO posts(id, author_id, title, description, content, inserted_date) VALUES
(4,4,'itaque','deleniti','Magni nam optio id recusandae.','2010-07-28');
INSERT INTO posts(id, author_id, title, description, content, inserted_date) VALUES
(5,5,'ad','similique','Rerum tempore quis ut nesciunt qui excepturi est.','2006-10-09');
```

#### Executing the tests
First you need to build project - `mvn package` will do the job.

##### from IDE
From your IDE, right-click on the `liquibase.ext.cassandra.LiquibaseHarnessSuiteIT` test class present in `src/test/groovy` directory.
Doing so, will allow you to execute all the standard change object tests in the liquibase-test-harness as well as the
Cassandra specific change objects tests created exclusively to test this extension (You can find this in the
`src/test/resources/liquibase/harness/change/changelogs/cassandra` directory).

To run single test case, let's say `addColumn`, create JUnit configuration for `liquibase.harness.change.ChangeObjectTests` with arg `-DchangeObjects=addColumn`
More details about different options can be found in [liquibase-test-harness readme](https://github.com/liquibase/liquibase-test-harness)

##### from command line
You can use `mvn verify` command to run all integration tests - both living in Cassandra and test-harness.
To run only test-harness IT test use `mvn -Dit.test=LiquibaseHarnessSuiteIT verify` command.

## Contributing

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

<a name="license"></a>
## License

This project is licensed under the Apache License Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

