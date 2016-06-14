package integration;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public class UpdateStandaloneCassandraIT {

    private static final String KEYSPACE = "liquibase";

    @Test
    public void canApplyChangelog() throws Exception {
        Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
        // Connect to the Cassandra instance setup by cassandra-unit
        Connection con = DriverManager.getConnection("jdbc:cassandra://127.0.0.1:9042/liquibase");
        // Trigger a liquibase update with a simple changelog
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(con));
        database.setDefaultSchemaName(KEYSPACE);
        Liquibase liquibase = new Liquibase("changeset/sql/changelog.sql", new ClassLoaderResourceAccessor(), database);
        Contexts contexts = new Contexts();
        liquibase.update(contexts);
    }

}
