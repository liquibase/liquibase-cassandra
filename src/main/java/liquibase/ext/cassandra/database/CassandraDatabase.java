package liquibase.ext.cassandra.database;

import com.simba.cassandra.cassandra.core.CDBJDBCConnection;
import com.simba.cassandra.jdbc.jdbc42.S42Connection;
import liquibase.Scope;
import liquibase.change.Change;
import liquibase.change.core.CreateTableChange;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.sql.visitor.SqlVisitor;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
	public void executeStatements(Change change, DatabaseChangeLog changeLog, List<SqlVisitor> sqlVisitors) throws LiquibaseException {


		super.executeStatements(change, changeLog, sqlVisitors);

		if (change instanceof CreateTableChange) {

			// table creation in AWS Keyspaces is not immediate like other Cassandras
			// https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-tables.html#tables-create
			// let's see if the DATABASECHANGELOG table is active before doing stuff
			//TODO improve this AWS check when we find out better way

			if (super.getConnection().getURL().toLowerCase().contains("amazonaws")) {
				int DBCL_GET_TABLE_ACTIVE_ATTEMPS = 10;
				while (DBCL_GET_TABLE_ACTIVE_ATTEMPS >= 0) {
					try {
						Statement statement = getStatement();
						ResultSet rs = statement.executeQuery("SELECT keyspace_name, table_name, status FROM " +
								"system_schema_mcs.tables WHERE keyspace_name = '" + getDefaultCatalogName() +
								"' AND table_name = '" + ((CreateTableChange) change).getTableName() + "'");
						while (rs.next()) {
							String status = rs.getString("status");
							if (status.equals("ACTIVE")) {
								//table is active, we're done here
								return;
							} else if (status.equals("CREATING")) {
								Scope.getCurrentScope().getLog(this.getClass()).info("table status = CREATING");
								DBCL_GET_TABLE_ACTIVE_ATTEMPS--;
								TimeUnit.SECONDS.sleep(3);
							} else {
								Scope.getCurrentScope().getLog(this.getClass()).severe(String.format("%s table in %s state.", ((CreateTableChange) change).getTableName(), status));
								// something went very wrong, are we having issues with another Cassandra platform...?
								return;
							}

						}
					} catch (ClassNotFoundException | InterruptedException | SQLException e) {
						throw new DatabaseException(e);
					}

				}
			}

		}

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
