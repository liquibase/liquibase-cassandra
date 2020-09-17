liquibase-cassandra
===================

Liquibase extension for Cassandra Support


Prior to using Liquibase to manager your schema, you need to have Cassandra running and a keyspace created. Here are the steps to do that.

#### Start Cassandra
`docker run -p 9042:9042 --rm --name mycassandra -d cassandra`

#### Get the Cassandra Instance IP
`docker inspect mycassandra`

#### Start another instance for cqlsh

`docker run -it --rm cassandra bash`

`> cqlsh 172.17.0.2`

#### Create the keyspace
`cqlsh>CREATE KEYSPACE betterbotz WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};`
