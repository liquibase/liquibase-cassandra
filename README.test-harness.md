# Using the Liquibase Test Harness in the Cassandra Extension
The liquibase-snowflake extension now comes with integration test support via the liquibase-test-harness. 
This Liquibase test framework is designed to *also* make it easy for you to test your extensions.

### Configuring your project
 
#### Configuring your connections

We have provided a `harness-config.yml` file in `src/test/resources` directory. 
This file should contain the connection information for all the databases you want the Cassandra extension to be tested against.

#### Configuring test DB
The test-harness requires the database under test to be created beforehand, so that the tests might have some data to manipulate, are independent of each other and don't rely on any specific run order. 

To be able to run the harness tests locally setting up docker container is currently the simplest route. But if you already have a test instance running elsewhere, use the `test.cql` script to populate it with test data. 

To create a local test database docker container, execute the following steps:
- Run main cassandra instance `docker run -p 9042:9042 --rm --name mycassandra -d cassandra`, and give it few seconds to start
- Run `docker inspect mycassandra` to get a main instance IP address. By default, it's 172.17.0.2 but may change in our local env.
- To execute init script run second container `docker run -it --rm cassandra bash`
- enter cql console `cqlsh 172.17.0.2` (or other IP showed by `docker inspect mycassandra` if this doesn't work) 
- copy and paste `test.cql` file content to create keyspace and tables for tests. 

#### Executing the tests
First you need to build project - `mvn package` will do the job

##### from IDE
From your IDE, right click on the `liquibase.ext.cassandra.LiquibaseHarnessSuiteIT` test class present in `src/test/groovy` directory. 
Doing so, will allow you to execute all the standard change object tests in the liquibase-test-harness as well as the
Cassandra specific change objects tests created exclusively to test this extension (You can find this in the 
`src/test/resources/liquibase/harness/change/changelogs/cassandra` directory).

##### from command line
You can use `mvn verify` command to run all integration tests - both living in Cassandra and test-harness.
To run only test-harness IT test use `mvn -Dit.test=LiquibaseHarnessSuiteIT verify` command.
