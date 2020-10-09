package liquibase.ext.cassandra

import liquibase.Contexts
import liquibase.Liquibase
import liquibase.changelog.ChangeLogHistoryService
import liquibase.changelog.ChangeLogHistoryServiceFactory
import liquibase.changelog.RanChangeSet
import liquibase.integration.commandline.CommandLineUtils
import liquibase.resource.ClassLoaderResourceAccessor
import spock.lang.Specification

class FirstTest extends Specification {

    def "testing something"() {
        expect:
        true == true
    }

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

}
