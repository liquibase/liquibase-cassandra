package liquibase.ext.cassandra.lockservice

import liquibase.executor.Executor
import liquibase.ext.cassandra.database.CassandraDatabase
import liquibase.statement.SqlStatement
import liquibase.statement.core.RawSqlStatement
import liquibase.util.NetUtil
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Method

class LockServiceCassandraTest extends Specification {

    LockServiceCassandra lockService
    Executor executor
    CassandraDatabase database

    def setup() {
        lockService = new LockServiceCassandra()
        database = Mock(CassandraDatabase)
        executor = Mock(Executor)
        lockService.setDatabase(database)
        database.getLiquibaseCatalogName() >> "betterbotz"
        database.getDatabaseChangeLogLockTableName() >> "DATABASECHANGELOGLOCK"
    }

    // ─── isLocked ────────────────────────────────────────────────────────────

    def "isLocked queries by partition key, not by ALLOW FILTERING"() {
        given:
        executor.queryForList(_) >> []

        when:
        callIsLocked()

        then:
        1 * executor.queryForList({ SqlStatement stmt ->
            def sql = (stmt as RawSqlStatement).sql.toUpperCase()
            sql.contains("WHERE ID = 1") && !sql.contains("ALLOW FILTERING")
        }) >> []
    }

    @Unroll
    def "isLocked returns #expected when LOCKED = #lockedValue"() {
        given:
        executor.queryForList(_) >> [[(locked): lockedValue]]

        expect:
        callIsLocked() == expected

        where:
        locked   | lockedValue    | expected
        "LOCKED" | true           | true
        "LOCKED" | false          | false
        "locked" | Boolean.TRUE   | true
        "locked" | Boolean.FALSE  | false
        "LOCKED" | 1              | true
        "LOCKED" | 0              | false
    }

    def "isLocked returns false when the lock table row is absent"() {
        given:
        executor.queryForList(_) >> []

        expect:
        !callIsLocked()
    }

    // ─── lockAcquisitionInconclusive ─────────────────────────────────────────

    @Unroll
    def "lockAcquisitionInconclusive returns #expected for rowsUpdated=#rowsUpdated"() {
        expect:
        callLockAcquisitionInconclusive(rowsUpdated) == expected

        where:
        rowsUpdated | expected
        -1          | true   // driver reports "unknown affected rows" for the LWT-backed UPDATE
        0           | true   // some JDBC option sets report 0 even when the conditional apply succeeded
        1           | false  // unambiguous single-row success
    }

    // ─── isLockedByCurrentInstance ───────────────────────────────────────────

    def "isLockedByCurrentInstance queries by partition key, not by ALLOW FILTERING"() {
        given:
        executor.queryForList(_) >> []

        when:
        callIsLockedByCurrentInstance()

        then:
        1 * executor.queryForList({ SqlStatement stmt ->
            def sql = (stmt as RawSqlStatement).sql.toUpperCase()
            sql.contains("WHERE ID = 1") && !sql.contains("ALLOW FILTERING")
        }) >> []
    }

    def "isLockedByCurrentInstance returns false when the lock table row is absent"() {
        given:
        executor.queryForList(_) >> []

        expect:
        !callIsLockedByCurrentInstance()
    }

    def "isLockedByCurrentInstance returns false when LOCKED = false"() {
        given:
        executor.queryForList(_) >> [[LOCKED: false, LOCKEDBY: currentLockedBy()]]

        expect:
        !callIsLockedByCurrentInstance()
    }

    def "isLockedByCurrentInstance returns false when locked by a different host"() {
        given:
        executor.queryForList(_) >> [[LOCKED: true, LOCKEDBY: "otherhost (9.9.9.9)"]]

        expect:
        !callIsLockedByCurrentInstance()
    }

    def "isLockedByCurrentInstance returns true when locked by this host"() {
        given:
        executor.queryForList(_) >> [[LOCKED: true, LOCKEDBY: currentLockedBy()]]

        expect:
        callIsLockedByCurrentInstance()
    }

    def "isLockedByCurrentInstance handles lowercase column names from JDBC driver"() {
        given:
        executor.queryForList(_) >> [[locked: true, lockedby: currentLockedBy()]]

        expect:
        callIsLockedByCurrentInstance()
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    private boolean callIsLocked() {
        Method m = LockServiceCassandra.getDeclaredMethod("isLocked", Executor)
        m.accessible = true
        return m.invoke(lockService, executor) as boolean
    }

    private boolean callIsLockedByCurrentInstance() {
        Method m = LockServiceCassandra.getDeclaredMethod("isLockedByCurrentInstance", Executor)
        m.accessible = true
        return m.invoke(lockService, executor) as boolean
    }

    private boolean callLockAcquisitionInconclusive(int rowsUpdated) {
        Method m = LockServiceCassandra.getDeclaredMethod("lockAcquisitionInconclusive", int)
        m.accessible = true
        return m.invoke(lockService, rowsUpdated) as boolean
    }

    private static String currentLockedBy() {
        "${NetUtil.getLocalHostName()} (${NetUtil.getLocalHostAddress()})"
    }
}
