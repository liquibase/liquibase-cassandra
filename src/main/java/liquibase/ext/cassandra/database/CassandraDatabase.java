package liquibase.ext.cassandra.database;

import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
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

	private String keyspace;

	@Override
	public String getShortName() {
		return "cassandra";
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
		return "com.simba.cassandra.jdbc42.Driver";
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
	public boolean isAutoCommit(){
		return true;
	}

	@Override
	public void setAutoCommit(boolean b){
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

	public String getKeyspace() {
		if (keyspace == null) {
			try {
				if (this.getConnection() instanceof JdbcConnection) {
					keyspace = ((JdbcConnection)this.getConnection()).getUnderlyingConnection().getSchema();          
				}
			} catch (Exception e) {
				Scope.getCurrentScope().getLog(CassandraDatabase.class)
						.severe("Could not get keyspace from connection", e);

			}
		}
		return keyspace;

	}

	@Override
	public boolean supportsSchemas() {
		return false;
	}

	/**
	 * Cassandra actually doesn't support neither catalogs nor schemas, but keyspaces.
	 * As default liquibase classes don't know what is keyspace we gonna use keyspace instead of catalog
	 */
	@Override
	public String getDefaultCatalogName() {
		return getKeyspace();
	}

	public Statement getStatement() throws DatabaseException {
		return ((JdbcConnection) super.getConnection()).createStatement();
	}

	public boolean hasDatabaseChangeLogLockTable() {
		boolean hasChangeLogLockTable;
		try {
			Statement statement = getStatement();
			statement.executeQuery("SELECT ID from " + getDefaultCatalogName() + ".DATABASECHANGELOGLOCK");
			statement.close();
			hasChangeLogLockTable = true;
		} catch (SQLException e) {
			Scope.getCurrentScope().getLog(getClass()).info("No DATABASECHANGELOGLOCK available in cassandra.");
			hasChangeLogLockTable = false;
		} catch (DatabaseException e) {
			e.printStackTrace();
			hasChangeLogLockTable = false;
		}

		// needs to be generated up front
		return hasChangeLogLockTable;
	}

	@Override
	public boolean jdbcCallsCatalogsSchemas() {
		return true;
	}

	@Override
	public boolean supportsNotNullConstraintNames() {
		return false;
	}

	@Override
	public boolean supportsPrimaryKeyNames() {
		return false;
	}
}
