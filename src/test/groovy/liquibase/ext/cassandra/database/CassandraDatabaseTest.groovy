package liquibase.ext.cassandra.database

import liquibase.Scope
import liquibase.configuration.LiquibaseConfiguration
import spock.lang.Specification

class CassandraDatabaseTest extends Specification {

    def getShortName() {
        expect:
        new CassandraDatabase().getShortName() == "cassandra"
    }

    def getDefaultDriver() {
        expect:
        new CassandraDatabase().getDefaultDriver(null) == null
        new CassandraDatabase().getDefaultDriver("jdbc:mysql://localhost") == null
        new CassandraDatabase().getDefaultDriver("jdbc:cassandra://localhost") != null
    }

    def isAwsKeyspacesCompatibilityModeDisabledByDefault() {
        expect:
        !CassandraDatabase.isAwsKeyspacesCompatibilityModeEnabled()
    }

}
