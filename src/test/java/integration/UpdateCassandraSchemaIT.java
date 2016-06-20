package integration;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public class UpdateCassandraSchemaIT {

    private static final Logger LOG = LogFactory.getInstance().getLog(UpdateCassandraSchemaIT.class.getName());

    private static final String KEYSPACE = "liquibase";
    private static final String CASSANDRA_CONFIGURATION_FILE = "cassandra.yaml";
    private static final String LIQUIBASE_CHANGELOG_FILE = "changeset/sql/changelog.sql";
    private static final String CASSANDRA_JDBC_URL = "jdbc:cassandra://127.0.0.1:9142/";

    // Sets up an embedded instance of Cassandra with a new empty keyspace
    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("empty.cql", KEYSPACE),CASSANDRA_CONFIGURATION_FILE);

    @Test
    public void canApplyChangelog() throws Exception {
        // Connect to the Cassandra instance setup by cassandra-unit
        Connection con = DriverManager.getConnection(CASSANDRA_JDBC_URL + KEYSPACE);
        // Trigger a liquibase update with a simple changelog
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(con));
        database.setDefaultSchemaName(KEYSPACE);
        Liquibase liquibase = new Liquibase(LIQUIBASE_CHANGELOG_FILE, new ClassLoaderResourceAccessor(), database);
        Contexts contexts = new Contexts();
        liquibase.update(contexts);
    }

}
