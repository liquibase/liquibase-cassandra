package integration;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public class UpdateEmbeddedCassandraIT {

    private static final String KEYSPACE = "liquibase";

    // Sets up an instance of Cassandra with a new empty keyspace
    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("empty.cql", KEYSPACE));

    @Test
    public void canApplyChangelog() throws Exception {
        Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
        // Connect to the Cassandra instance setup by cassandra-unit
        Connection con = DriverManager.getConnection("jdbc:cassandra://127.0.0.1:9142/" + KEYSPACE);
        // Trigger a liquibase update with a simple changelog
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(con));
        database.setDefaultSchemaName(KEYSPACE);
        Liquibase liquibase = new Liquibase("changelog.sql", new ClassLoaderResourceAccessor(), database);
        Contexts contexts = new Contexts();
        liquibase.update(contexts);
    }

}
