# Using the Liquibase Test Harness in the Cassandra Extension
The liquibase-snowflake extension now comes with integration test support via the liquibase-test-harness. 
This Liquibase test framework is designed to *also* make it easy for you to test your extensions.

### Configuring your project
 
#### Configuring your connections

We have provided a `harness-config.yml` file in `src/test/resources` directory. 
This file should contain the connection information for all the databases you want the Cassandra extension to be tested against.

#### Configuring test DB
Test-harness requires the database under test to be created beforehand, so that the tests might have some data to manipulate, are independent of each other and don't rely on any specific run order.
To be able to run the harness tests locally setting up docker container is currently the simplest route. But if you already have a test instance running elsewhere, use the `test.cql` script to populate it with test data.
To create a local test database docker container, execute the following steps:
- Run main cassandra instance `docker run -p 9042:9042 --rm --name mycassandra -d cassandra`, and give it few seconds to start
- To execute init script run second container `docker run -it --rm cassandra bash`
- enter cql console `cqlsh 172.17.0.2`
- copy and paste `test.cql` file content

#### Executing the tests
From your IDE, right click on the `LiquibaseHarnessSuiteTest` test class present in `src/test/groovy` directory. 
Doing so, will allow you to execute all the standard change object tests in the liquibase-test-harness as well as the
Cassandra specific change objects tests created exclusively to test this extension (You can find this in the 
`src/test/resources/liquibase/harness/change/changelogs/cassandra` directory).
