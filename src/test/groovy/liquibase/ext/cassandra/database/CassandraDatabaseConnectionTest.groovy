package liquibase.ext.cassandra.database

import com.ing.data.cassandra.jdbc.CassandraDriver
import liquibase.exception.DatabaseException
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Driver

class CassandraDatabaseConnectionTest extends Specification {

    @Unroll
    def "open with #url"() {
        given:
        def cassandraConnection = Spy(new CassandraDatabaseConnection() {
            // Overriding the real connection call, we just want to test it's called with the expected URL.
            @Override
            void openConnection(String jdbcUrl, Driver driverObject, Properties driverProperties) throws DatabaseException {
                println("Mock open connection with URL: $jdbcUrl")
            }
        })
        def cassandraDriver = new CassandraDriver()
        def cassandraDriverProperties = new Properties()

        when:
        cassandraConnection.open(url, cassandraDriver, cassandraDriverProperties)

        then:
        1 * cassandraConnection.openConnection(expectedUrl, cassandraDriver, cassandraDriverProperties)

        where:
        url                                                                                               | expectedUrl
        "jdbc:cassandra://localhost:9042/betterbotz?compliancemode=Liquibase&localdatacenter=datacenter1" | "jdbc:cassandra://localhost:9042/betterbotz?compliancemode=Liquibase&localdatacenter=datacenter1"
        "jdbc:cassandra://localhost:9042/betterbotz?localdatacenter=datacenter1"                          | "jdbc:cassandra://localhost:9042/betterbotz?localdatacenter=datacenter1&compliancemode=Liquibase"
        "jdbc:cassandra://localhost:9042/betterbotz"                                                      | "jdbc:cassandra://localhost:9042/betterbotz?compliancemode=Liquibase"

    }

}
