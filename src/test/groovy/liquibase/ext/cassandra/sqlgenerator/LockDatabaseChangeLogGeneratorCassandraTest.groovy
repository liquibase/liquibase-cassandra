package liquibase.ext.cassandra.sqlgenerator

import liquibase.ext.cassandra.database.CassandraDatabase
import liquibase.statement.core.LockDatabaseChangeLogStatement
import spock.lang.Specification

class LockDatabaseChangeLogGeneratorCassandraTest extends Specification {

    def "generateSql includes IF LOCKED = FALSE to prevent concurrent lock acquisition"() {
        given:
        def generator = new LockDatabaseChangeLogGeneratorCassandra()
        def database = Mock(CassandraDatabase)
        database.escapeTableName(_, _, _) >> "betterbotz.DATABASECHANGELOGLOCK"
        database.getDatabaseProductName() >> "Cassandra"

        when:
        def sql = generator.generateSql(new LockDatabaseChangeLogStatement(), database, null)

        then:
        sql.length > 0
        sql[0].toSql().toUpperCase().contains("IF LOCKED = FALSE")
    }

    def "generateSql does not produce an unconditional UPDATE that would allow split-brain"() {
        given:
        def generator = new LockDatabaseChangeLogGeneratorCassandra()
        def database = Mock(CassandraDatabase)
        database.escapeTableName(_, _, _) >> "betterbotz.DATABASECHANGELOGLOCK"
        database.getDatabaseProductName() >> "Cassandra"

        when:
        def sql = generator.generateSql(new LockDatabaseChangeLogStatement(), database, null)

        then:
        // The update must end with the IF condition, not a bare WHERE ID = 1
        !sql[0].toSql().trim().toUpperCase().endsWith("WHERE ID = 1")
    }
}
