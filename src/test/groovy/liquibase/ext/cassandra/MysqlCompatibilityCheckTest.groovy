package liquibase.ext.cassandra

import liquibase.Liquibase
import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.DatabaseException
import liquibase.exception.LiquibaseException
import liquibase.resource.ClassLoaderResourceAccessor
import org.h2.jdbcx.JdbcDataSource
import org.h2.tools.Server
import spock.lang.Specification

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

class MysqlCompatibilityCheckTest extends  Specification{

    /**
     * This test show what happens without the fix on mysql compatibility
     * Error: "TRUNCATE PUBLIC[*].DATABASECHANGELOGLOCK"; expected "TABLE";
     *
     */
    def "Liquibase should continue to work with Mysql when Cassandra Extension is in classpath"() throws SQLException, LiquibaseException {

        given: "starting H2 and initializing liquibase"
        JdbcDataSource dataSource = startH2()
        Liquibase liquibase = initLiquibase(dataSource)

        when: "apply test changelog"
        liquibase.update("")

        then: "selecting row count from table should match number of inserted rows in changeset"
        Statement stmt = dataSource.getConnection().createStatement()
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST_SCHEMA.products")
        rs.next()
        rs.getInt("COUNT(*)")==2
    }

    private static Liquibase initLiquibase(JdbcDataSource ds) throws SQLException, DatabaseException {
        Connection connection = ds.getConnection()
        Database database = DatabaseFactory
                .getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection))
        return new Liquibase(
                "mysql-changelog.xml",
                new ClassLoaderResourceAccessor(),
                database
        );
    }

    private static JdbcDataSource startH2() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource()
        ds.setURL("jdbc:h2:mem:test;MODE=Mysql")
        ds.setUser("sa")
        ds.setPassword("sa")
        Server server = Server.createWebServer("-webAllowOthers")
        server.start()
        return ds
    }

}
