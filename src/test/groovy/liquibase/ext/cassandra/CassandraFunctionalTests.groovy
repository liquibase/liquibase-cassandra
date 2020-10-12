package liquibase.ext.cassandra

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.change.CheckSum
import liquibase.changelog.ChangeLogHistoryServiceFactory
import liquibase.integration.commandline.CommandLineUtils
import liquibase.resource.ClassLoaderResourceAccessor
import spock.lang.Specification

class CassandraFunctionalTests extends Specification {

    def "calculateCheckSum"() {
        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz"
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null)
        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database)
        CheckSum checkSum = liquibase.calculateCheckSum("changelog.xml", "1", "betterbotz")
        then:
        //TODO: need seperate changelog that actual has some stuff that needs to be sync'd
        assert checkSum.toString().trim() == "8:80f1a851837367ff74bdb07075c716af"

    }

    def "changeLogSync"() {

        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz"
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null)
        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database)
        def stringWriter = new StringWriter()
        liquibase.changeLogSync((Contexts) null, (LabelExpression) null)
        then:
        database != null;

    }

    // TODO: need to make clearCheckSums extendable
    //  https://github.com/liquibase/liquibase/issues/1472
//    def "clearCheckSums"() {
//
//        when:
//        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
//        def defaultSchemaName = "betterbotz"
//        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null)
//        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database)
//        def stringWriter = new StringWriter()
//        liquibase.clearCheckSums();
//        then:
//        database != null;
//
//    }

    def "update"() {

        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz";
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null);
        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update((Contexts) null);
        then:
        database != null;

    }

    def "changeLogSyncSQL"() {

        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz"
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null)
        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database)
        def stringWriter = new StringWriter()
        liquibase.changeLogSync((Contexts) null, (LabelExpression) null, stringWriter)
        then:
        assert stringWriter.toString().contains("UPDATE betterbotz.DATABASECHANGELOGLOCK SET LOCKED = TRUE")

    }

    def "status"() {

        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz";
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null);
        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database);
        def statusOutput = new StringWriter()
        liquibase.reportStatus(false, (Contexts) null, statusOutput);
        then:
        statusOutput.toString().trim() == "@jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz is up to date";

    }

    def "status2"() {

        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz";
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null);
        def historyService = ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database);
        def ranChangeSets = historyService.getRanChangeSets();
        then:
        ranChangeSets.size() == 3;

    }

    def "dbDoc"() {

        when:
        def url = "jdbc:cassandra://localhost:9042/betterbotz;DefaultKeyspace=betterbotz"
        def defaultSchemaName = "betterbotz";
        def database = CommandLineUtils.createDatabaseObject(new ClassLoaderResourceAccessor(), url, null, null, null, null, defaultSchemaName, false, false, null, null, null, null, null, null, null);
        def liquibase = new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.generateDocumentation("target");
        then:
        database != null;

    }

}
