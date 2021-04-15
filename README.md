liquibase-cassandra
===================

Liquibase extension for Cassandra Support.


### Setup local test environment

start cassandra

`docker run -p 9042:9042 --rm --name mycassandra -d cassandra`

get ip

`docker inspect mycassandra`
 
start another instance for cqlsh, check for IP address, replace mentioned IP with the one inspect command shows

`docker run -it --rm cassandra bash`

`> cqlsh 172.17.0.2`

execute the following CQL:

```
CREATE KEYSPACE betterbotz
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };
```
