package liquibase.ext.cassandra;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class MysqlCompatibilityCheckTest {

    /**
     * This test show what happens without the fix on mysql compatibility
     * Error: "TRUNCATE PUBLIC[*].DATABASECHANGELOGLOCK"; expected "TABLE";
     *
     */
    @Test
    void liquibaseShouldContinueToWorkWithMysqlWhenCassExtIsInClasspath() throws SQLException, LiquibaseException {

        // GIVEN
        JdbcDataSource dataSource = startH2();
        Liquibase liquibase = initLiquibase(dataSource);

        // WHEN
        liquibase.update("");

        // THEN
        Statement stmt = dataSource.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST_SCHEMA.products");
        assertThat(rs.getRow()).isZero();
    }

    private Liquibase initLiquibase(JdbcDataSource ds) throws SQLException, DatabaseException {
        Connection connection = ds.getConnection();
        Database database = DatabaseFactory
                .getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection));
        return new Liquibase(
                "mysql-changelog.xml",
                new ClassLoaderResourceAccessor(),
                database
        );
    }

    private JdbcDataSource startH2() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test;MODE=Mysql");
        ds.setUser("sa");
        ds.setPassword("sa");
        Server server = Server.createWebServer("-webAllowOthers");
        server.start();
        return ds;
    }

}
