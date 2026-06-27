package liquibase.ext.cassandra.changelog

import liquibase.database.Database
import spock.lang.Specification

class CassandraChangeLogHistoryServiceTest extends Specification {

    def "tagExists returns true when a changeset with the matching tag exists"() {
        given:
        def service = Spy(CassandraChangeLogHistoryService) {
            hasDatabaseChangeLogTable() >> true
            getDatabase() >> Mock(Database)
            queryDatabaseChangeLogTable(_) >> [
                [ID: "1", AUTHOR: "dev", FILENAME: "db.sql", TAG: "v1.0"],
                [ID: "2", AUTHOR: "dev", FILENAME: "db.sql", TAG: null]
            ]
        }

        expect:
        service.tagExists("v1.0")
    }

    def "tagExists returns false when no changeset carries the requested tag"() {
        given:
        def service = Spy(CassandraChangeLogHistoryService) {
            hasDatabaseChangeLogTable() >> true
            getDatabase() >> Mock(Database)
            queryDatabaseChangeLogTable(_) >> [
                [ID: "1", AUTHOR: "dev", FILENAME: "db.sql", TAG: "v1.0"]
            ]
        }

        expect:
        !service.tagExists("v2.0")
    }

    def "tagExists returns false when the DATABASECHANGELOG table does not exist"() {
        given:
        def service = Spy(CassandraChangeLogHistoryService) {
            hasDatabaseChangeLogTable() >> false
        }

        expect:
        !service.tagExists("v1.0")
    }

    def "tagExists handles null TAG values without throwing"() {
        given:
        def service = Spy(CassandraChangeLogHistoryService) {
            hasDatabaseChangeLogTable() >> true
            getDatabase() >> Mock(Database)
            queryDatabaseChangeLogTable(_) >> [
                [ID: "1", AUTHOR: "dev", FILENAME: "db.sql", TAG: null],
                [ID: "2", AUTHOR: "dev", FILENAME: "db.sql", TAG: null]
            ]
        }

        expect:
        !service.tagExists("v1.0")
    }
}
