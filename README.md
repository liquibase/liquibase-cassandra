liquibase-cassandra[![Build and Test Extention](https://github.com/liquibase/liquibase-cassandra/actions/workflows/build.yml/badge.svg)](https://github.com/liquibase/liquibase-cassandra/actions/workflows/build.yml)
===================

Liquibase extension for Cassandra Support.


### Setup local test environment with Liquibase Test Harness

start cassandra

`docker run -p 9042:9042 --rm --name cassandra3 -d cassandra:3`

`docker run -p 9043:9042 --rm --name cassandra4 -d cassandra:4.0`

NOTE: Cassandra 4.0-rc1 is not behaving on Windows 10, Docker Desktop 3.3.3. Have run successfully on Centos 8.

get ip

`docker inspect cassandra3`

`docker inspect cassandra4.0`
 
start another instance for cqlsh, check for IP address, replace mentioned IP with the one inspect command shows. Repeat for other Cassandra instance(s).

`docker run -it --rm cassandra bash`

`> cqlsh 172.17.0.2`

execute the following CQL (from test.cql):

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
(5,5,'ad','similique','Rerum tempore quis ut nesciunt qui excepturi est.','2006-10-09');;
```

## Contributing

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

<a name="license"></a>
## License

This project is licensed under the Apache License Version 2.0 - see the [LICENSE.md](LICENSE.md) file for details

