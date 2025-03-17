package liquibase.ext.cassandra.snapshot

import liquibase.database.core.MockDatabase
import liquibase.ext.cassandra.database.CassandraDatabase
import liquibase.structure.core.ForeignKey
import liquibase.structure.core.Table
import spock.lang.Specification
import spock.lang.Subject

class ForeignKeySnapshotGeneratorCassandraTest extends Specification {

    @Subject
    ForeignKeySnapshotGeneratorCassandra generator = new ForeignKeySnapshotGeneratorCassandra()

    def "check if the generator has priority for Cassandra databases"() {
        given:
        def cassandraDb = new CassandraDatabase()
        def mockDb = new MockDatabase()

        expect:
        generator.getPriority(ForeignKey.class, cassandraDb) > generator.getPriority(ForeignKey.class, mockDb)
        generator.getPriority(ForeignKey.class, mockDb) == generator.PRIORITY_NONE
    }

    def "Addto should not do anything for Cassandra"() {
        given:
        def table = new Table()
        when:

        generator.addTo(table, null)
        
        then:
        noExceptionThrown()
    }

    def "Snapshotobject should return null to Cassandra"() {
        given:
        def foreignKey = new ForeignKey()
        
        when:
        def result = generator.snapshotObject(foreignKey, null)
        
        then:
        result == null
    }
}