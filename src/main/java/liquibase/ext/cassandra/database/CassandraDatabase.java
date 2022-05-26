package liquibase.ext.cassandra.database;

import com.simba.cassandra.cassandra.core.CDBJDBCConnection;
import com.simba.cassandra.jdbc.jdbc42.S42Connection;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

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
	public int getDatabaseMinorVersion() throws DatabaseException {
		return 0;
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
		if (String.valueOf(url).startsWith("jdbc:cassandra:")) {
			return "com.simba.cassandra.jdbc42.Driver";
		}
		return null;
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
					keyspace = ((CDBJDBCConnection) ((S42Connection) ((JdbcConnection) (this).getConnection())
							.getUnderlyingConnection()).getConnection()).getSession().getLoggedKeyspace();
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
