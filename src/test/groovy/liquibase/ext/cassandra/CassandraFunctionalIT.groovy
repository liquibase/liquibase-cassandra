package liquibase.ext.cassandra

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.change.CheckSum
import liquibase.changelog.ChangeLogHistoryServiceFactory
import liquibase.integration.commandline.CommandLineUtils
import liquibase.resource.ClassLoaderResourceAccessor
import spock.lang.Specification


class CassandraFunctionalIT extends Specification {

    def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
    def defaultSchemaName = "betterbotz"
    def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null)

    def "calculateCheckSum"() {
        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        CheckSum checkSum = liquibase.calculateCheckSum("test-changelog.xml", "1", "betterbotz")
        then:
        //TODO: need seperate changelog that actual has some stuff that needs to be sync'd
        assert checkSum.toString().trim() == "8:80f1a851837367ff74bdb07075c716af"

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
        assert stringWriter.toString().contains("UPDATE betterbotz.databasechangeloglock SET LOCKED = TRUE")

    }

    def "status"() {

        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        def statusOutput = new StringWriter()
        liquibase.reportStatus(false, (Contexts) null, statusOutput)
        then:
        statusOutput.toString().trim() == "@jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz is up to date"

    }

    def "status2"() {

        when:
        def historyService = ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database)
        def ranChangeSets = historyService.getRanChangeSets()
        then:
        ranChangeSets.size() == 3

    }

    def "dbDoc"() {

        when:
        def liquibase = new Liquibase("test-changelog.xml", new ClassLoaderResourceAccessor(), database)
        liquibase.generateDocumentation("target")
        then:
        database != null

    }

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
    }

}
