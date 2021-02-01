# Using the Liquibase Test Harness in the Cassandra Extension
The liquibase-snowflake extension now comes with integration test support via the liquibase-test-harness. 
This Liquibase test framework is designed to *also* make it easy for you to test your extensions.

### Configuring your project
 
#### Configuring your connections

We have provided a `harness-config.yml` file in `src/test/resources` directory. 
This file should contain the connection information for all the databases you want the Cassandra extension to be tested against.

#### Configuring test DB
Test-harness requires test DB to be created beforehand, so tests have some data to manipulate, are independent and don't rely on specific run order.
To be able to run test locally setting up docker container might be the easiest way. If you already have test instance running somewhere, use `test.cql` to fill it with test data
To create local docker container do the following steps:
- Run main cassandra instance `docker run -p 9042:9042 --rm --name mycassandra -d cassandra`, give it few seconds to start
- To execute init script run second container `docker run -it --rm cassandra bash`
- enter cql console `cqlsh 172.17.0.2`
- copy and paste `test.cql` file content

#### Executing the tests
From your IDE, right click on the `LiquibaseHarnessSuiteTest` test class present in `src/test/groovy` directory. 
Doing so, will allow you to execute all the standard change object tests in the liquibase-test-harness as well as the
Cassandra specific change objects tests created exclusively to test this extension (You can find this in the 
`src/test/resources/liquibase/harness/change/changelogs/cassandra` directory).