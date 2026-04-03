package liquibase.ext.cassandra

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.Scope
import liquibase.change.CheckSum
import liquibase.changelog.ChangeLogHistoryServiceFactory
import liquibase.changelog.ChangeSet
import liquibase.changelog.RanChangeSet
import liquibase.exception.CommandExecutionException
import liquibase.executor.Executor
import liquibase.executor.ExecutorService
import liquibase.ext.cassandra.changelog.CassandraChangeLogHistoryService
import liquibase.ext.cassandra.lockservice.LockServiceCassandra
import liquibase.integration.commandline.CommandLineUtils
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.statement.core.RawParameterizedSqlStatement
import spock.lang.Specification

class CassandraFunctionalIT extends Specification {
    def url = System.getProperty("dbUrl", "jdbc:cassandra://localhost:9043/betterbotz?compliancemode=Liquibase&localdatacenter=datacenter1")
    def defaultSchemaName = "betterbotz"
    def username = System.getProperty("dbUsername", "cassandra")
    def password = System.getProperty("dbPassword", "Password1")
    def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, username, password, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null)
    def historyService = new CassandraChangeLogHistoryService()

    def "calculateCheckSum"() {
        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        CheckSum checkSum = liquibase.calculateCheckSum("test-changelog.xml", "1", "betterbotz")
        then:
        //TODO: need seperate changelog that actual has some stuff that needs to be sync'd
        assert checkSum.toString().trim() == "9:8f52111d40b85d50739aec05bafdbc2f"

    }

    def "changeLogSync"() {

        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        def stringWriter = new StringWriter()
        liquibase.changeLogSync((Contexts) null, (LabelExpression) null)
        then:
        database != null

    }

    // TODO: need to make clearCheckSums extendable
    //  https://github.com/liquibase/liquibase/issues/1472
//    def "clearCheckSums"() {
//
//        when:
//        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
//        def stringWriter = new StringWriter()
//        liquibase.clearCheckSums();
//        then:
//        database != null;
//
//    }

    def "update"() {

        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        liquibase.update((Contexts) null)
        then:
        database != null

    }

    def "changeLogSyncSQL"() {

        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        def stringWriter = new StringWriter()
        liquibase.changeLogSync((Contexts) null, (LabelExpression) null, stringWriter)
        then:
        assert stringWriter.toString().contains("UPDATE betterbotz.DATABASECHANGELOGLOCK SET LOCKED = TRUE")

    }

    def "status"() {

        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        def statusOutput = new StringWriter()
        liquibase.reportStatus(false, (Contexts) null, statusOutput)
        def urlWithoutParameters = url.split("\\?")[0]
        then:
        statusOutput.toString().trim() == "$username@$urlWithoutParameters is up to date"

    }

    def "status2"() {

        when:
        def historyService = ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database)
        def ranChangeSets = historyService.getRanChangeSets()
        then:
        ranChangeSets.size() == 8

    }

//    def "dbDoc"() {
//
//        when:
//        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
//        liquibase.generateDocumentation("target")
//        then:
//        database != null
//
//    }

    def "rollbackCount1"() {

        when:
            def liquibase = new Liquibase("rollback.changelog.sql", new ClassLoaderResourceAccessor(), database)
            liquibase.update((Contexts) null)
            def output = new StringWriter()
            liquibase.rollback(1, (String) null, output)
        then:
            assert output.toString().trim().contains("DROP TABLE betterbotz.TESTME5")
        when:
            liquibase.rollback(1, (String) null)
        then:
            database != null
            thrown(CommandExecutionException.class)
    }

    def "listLocks"() {

        when:
            def lockService = new LockServiceCassandra()
            lockService.setDatabase(database)
            lockService.init()
            lockService.acquireLock()
            def locks = lockService.listLocks()
        then:
            try {
                assert locks.size() > 0
            } finally {
                lockService.forceReleaseLock()
            }
    }

    def "determines empty database is checksum-compatible"() {
        when:
        historyService.setDatabase(database)
        historyService.init()

        then:
        historyService.isDatabaseChecksumsCompatible()
    }

    def "determines change set with older checksum database is not checksum-compatible"() {
        when:
        historyService.setDatabase(database)
        def v1CheckSum = CheckSum.parse("1:2cdf9876e74347162401315d34b83746")
        manuallyCreateChangeset(ranChangeSet("old-change-set", "me", v1CheckSum, new Date()))

        and:
        historyService.init()

        then:
        !historyService.getIncompatibleDatabaseChangeLogs().empty
    }

    private static RanChangeSet ranChangeSet(String id,
                                             String author,
                                             CheckSum checkSum = null,
                                             Date date,
                                             String logicalChangeLogPath = "some/logical/path",
                                             String physicalChangeLogPath = "some/physical/path") {
        def ranChangeSet = new RanChangeSet(logicalChangeLogPath,
                id,
                author,
                checkSum,
                date,
                null,
                ChangeSet.ExecType.EXECUTED,
                "some description",
                "some comments",
                null,
                null,
                "some deployment ID",
                physicalChangeLogPath)
        ranChangeSet.liquibaseVersion = "5.0.2"
        return ranChangeSet
    }

    private manuallyCreateChangeset(RanChangeSet changeSet) {
        def query = "INSERT INTO DATABASECHANGELOG(ID, AUTHOR, FILENAME, COMMENTS, CONTEXTS, DATEEXECUTED, " +
                "ORDEREXECUTED, DEPLOYMENT_ID, DESCRIPTION, EXECTYPE, LABELS, LIQUIBASE, MD5SUM, TAG) VALUES (" +
                "'${changeSet.id}', '${changeSet.author}', '${changeSet.changeLog}', '${changeSet.comments}', '', " +
                "${changeSet.dateExecuted.time}, 1, '${changeSet.deploymentId}', '${changeSet.description}', " +
                "'${changeSet.execType.name()}', '${changeSet.labels.toString()}', '${changeSet.liquibaseVersion}', " +
                "'${changeSet.lastCheckSum.toString()}', '${changeSet.tag}')"

        def jdbcExecutor = (Executor) Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        jdbcExecutor.execute(new RawParameterizedSqlStatement(query))
    }

}
