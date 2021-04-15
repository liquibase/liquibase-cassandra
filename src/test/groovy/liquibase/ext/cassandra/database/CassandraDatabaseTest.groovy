package liquibase.ext.cassandra.database

import spock.lang.Specification

class CassandraDatabaseTest extends Specification {

    def getShortName() {
        expect:
        new CassandraDatabase().getShortName() == "cassandra"
    }

}
