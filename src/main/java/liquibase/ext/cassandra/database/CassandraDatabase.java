package liquibase.ext.cassandra.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Cassandra 1.2.0 NoSQL database support.
 */
public class CassandraDatabase extends AbstractJdbcDatabase {
	public static final String PRODUCT_NAME = "Cassandra";
	public static final String CASSANDRA_DRIVER = "com.github.cassandra.jdbc.CassandraDriver";

	@Override
	public String getShortName() {
		return "cassandra";
	}

	public CassandraDatabase() {
		setDefaultSchemaName("");
	}

	@Override
	public int getPriority() {
		return PRIORITY_DEFAULT;
	}

	@Override
	protected String getDefaultDatabaseProductName() {
		return "Cassandra";
	}

	@Override
	public Integer getDefaultPort() {
		return 9160;
	}

	@Override
	public boolean supportsInitiallyDeferrableColumns() {
		return false;
	}

	@Override
	public boolean supportsSequences() {
		return false;
	}

	@Override
	public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
		String databaseProductName = conn.getDatabaseProductName();
		return PRODUCT_NAME.equalsIgnoreCase(databaseProductName);
	}

	@Override
	public String getDefaultDriver(String url) {
		return CASSANDRA_DRIVER;
	}

	@Override
	public boolean supportsTablespaces() {
		return false;
	}

	@Override
	public boolean supportsRestrictForeignKeys() {
		return false;
	}

	@Override
	public boolean supportsDropTableCascadeConstraints() {
		return false;
	}

	@Override
	public boolean isAutoCommit() throws DatabaseException {
		return true;
	}

	@Override
	public void setAutoCommit(boolean b) throws DatabaseException {
	}

	@Override
	public boolean isCaseSensitive() {
		return true;
	}

	@Override
	public String getCurrentDateTimeFunction() {
		// no alternative in cassandra, using client time
		return String.valueOf(System.currentTimeMillis());
	}

	public Statement getStatement() throws ClassNotFoundException, SQLException {
		String url = super.getConnection().getURL();
		Class.forName(CASSANDRA_DRIVER);
		Connection con = DriverManager.getConnection(url);
		Statement statement = con.createStatement();
		return statement;
	}

}
