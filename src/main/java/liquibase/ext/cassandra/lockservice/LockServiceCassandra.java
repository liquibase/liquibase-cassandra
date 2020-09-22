package liquibase.ext.cassandra.lockservice;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.core.DB2Database;
import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.MSSQLDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.sqlgenerator.CassandraUtil;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.LogFactory;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.DropTableStatement;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LockServiceCassandra extends StandardLockService {

	private Boolean hasDatabaseChangeLogLockTable;
	private boolean isDatabaseChangeLogLockTableInitialized;
	private ObjectQuotingStrategy quotingStrategy;

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supports(Database database) {
		return database instanceof CassandraDatabase;
	}

	@Override
	public boolean acquireLock() throws LockException {

		if (super.hasChangeLogLock) {
			return true;
		}

		Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

		try {

			database.rollback();
			init();

			Boolean locked = executor
					.queryForInt(new RawSqlStatement("SELECT COUNT(*) FROM " + CassandraUtil.getKeyspace(database)
							+ ".DATABASECHANGELOGLOCK where locked = TRUE ALLOW FILTERING")) > 0;

			if (locked) {
				return false;
			} else {

				executor.comment("Lock Database");
				int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());
				if ((rowsUpdated == -1) && (database instanceof MSSQLDatabase)) {

					LogFactory.getLogger()
							.info("Database did not return a proper row count (Might have NOCOUNT enabled)");
					database.rollback();
					Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(new LockDatabaseChangeLogStatement(),
							database);
					if (sql.length != 1) {
						throw new UnexpectedLiquibaseException("Did not expect " + sql.length + " statements");
					}
					rowsUpdated = executor.update(new RawSqlStatement(
							"EXEC sp_executesql N'SET NOCOUNT OFF " + sql[0].toSql().replace("'", "''") + "'"));
				}
				if (rowsUpdated > 1) {
					throw new LockException("Did not update change log lock correctly");
				}
				if (rowsUpdated == 0) {
					// another node was faster
					return false;
				}
				database.commit();
				LogFactory.getLogger().info("successfully.acquired.change.log.lock");

				hasChangeLogLock = true;

				database.setCanCacheLiquibaseTableInfo(true);
				return true;
			}
		} catch (Exception e) {
			throw new LockException(e);
		} finally {
			try {
				database.rollback();
			} catch (DatabaseException e) {
			}
		}

	}

	@Override
	public void releaseLock() throws LockException {

		ObjectQuotingStrategy incomingQuotingStrategy = null;
		if (this.quotingStrategy != null) {
			incomingQuotingStrategy = database.getObjectQuotingStrategy();
			database.setObjectQuotingStrategy(this.quotingStrategy);
		}

		Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
		try {
			if (this.hasDatabaseChangeLogLockTable()) {
				executor.comment("Release Database Lock");
				database.rollback();
				int updatedRows = executor.update(new UnlockDatabaseChangeLogStatement());
				if ((updatedRows == -1) && (database instanceof MSSQLDatabase)) {
					LogFactory.getLogger()
							.info("Database did not return a proper row count (Might have NOCOUNT enabled.)");
					database.rollback();
					Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(new UnlockDatabaseChangeLogStatement(),
							database);
					if (sql.length != 1) {
						throw new UnexpectedLiquibaseException("Did not expect " + sql.length + " statements");
					}
					updatedRows = executor.update(new RawSqlStatement(
							"EXEC sp_executesql N'SET NOCOUNT OFF " + sql[0].toSql().replace("'", "''") + "'"));
				}
				if (updatedRows != 1) {
					throw new LockException("Did not update change log lock correctly.\n\n" + updatedRows
							+ " rows were updated instead of the expected 1 row using executor "
							+ executor.getClass().getName() + "" + " there are "
							+ executor.queryForInt(new RawSqlStatement(
									"SELECT COUNT(*) FROM " + database.getDatabaseChangeLogLockTableName()))
							+ " rows in the table");
				}
				database.commit();
			}
		} catch (Exception e) {
			throw new LockException(e);
		} finally {
			try {
				hasChangeLogLock = false;

				database.setCanCacheLiquibaseTableInfo(false);
				Scope.getCurrentScope().getLog(getClass()).info("Successfully released change log lock");
				database.rollback();
			} catch (DatabaseException e) {
			}
			if (incomingQuotingStrategy != null) {
				database.setObjectQuotingStrategy(incomingQuotingStrategy);
			}
		}
	}

	@Override
	public boolean hasDatabaseChangeLogLockTable() throws DatabaseException {
		boolean hasChangeLogLockTable;
		try {
			Statement statement = ((CassandraDatabase) database).getStatement();
			ResultSet rs = statement.executeQuery("SELECT table_name  FROM system_schema.tables WHERE keyspace_name='"
					+ CassandraUtil.getKeyspace(database) + "' AND table_name = 'databasechangeloglock'");
			// Cassandra will create the table as lower case even if you specify uppercase
			// in the create statement
			if (rs.next() == false) {
				hasChangeLogLockTable = false;
			} else {
				hasChangeLogLockTable = true;
			}
			statement.close();
		} catch (SQLException e) {
			Scope.getCurrentScope().getLog(getClass()).info("No DATABASECHANGELOGLOCK available in cassandra.");
			hasChangeLogLockTable = false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			hasChangeLogLockTable = false;
		}

		// needs to be generated up front
		return hasChangeLogLockTable;
	}

	@Override
	public boolean isDatabaseChangeLogLockTableInitialized(final boolean tableJustCreated) throws DatabaseException {
		if (!isDatabaseChangeLogLockTableInitialized) {
			Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",
					database);

			try {
				isDatabaseChangeLogLockTableInitialized = executor.queryForInt(new RawSqlStatement(
						"SELECT COUNT(*) FROM " + CassandraUtil.getKeyspace(database) + ".DATABASECHANGELOGLOCK")) > 0;
			} catch (LiquibaseException e) {
				if (executor.updatesDatabase()) {
					throw new UnexpectedLiquibaseException(e);
				} else {
					// probably didn't actually create the table yet.
					isDatabaseChangeLogLockTableInitialized = !tableJustCreated;
				}
			}
		}
		return isDatabaseChangeLogLockTableInitialized;
	}

	@Override
	public void init() throws DatabaseException {
		boolean createdTable = false;
		Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

		if (!hasDatabaseChangeLogLockTable()) {
			try {
				executor.comment("Create Database Lock Table");
				// TODO: create databsechangeloglock table
				executor.execute(new CreateDatabaseChangeLogLockTableStatement());

				database.commit();
				Scope.getCurrentScope().getLog(getClass())
						.fine("Created database lock table with name: " + database.escapeTableName(
								database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(),
								database.getDatabaseChangeLogLockTableName()));
			} catch (DatabaseException e) {
				if ((e.getMessage() != null) && e.getMessage().contains("exists")) {
					// hit a race condition where the table got created by another node.
					Scope.getCurrentScope().getLog(getClass()).fine("Database lock table already appears to exist "
							+ "due to exception: " + e.getMessage() + ". Continuing on");
				} else {
					throw e;
				}
			}
			this.hasDatabaseChangeLogLockTable = true;
			createdTable = true;
			hasDatabaseChangeLogLockTable = true;
		}

		if (!isDatabaseChangeLogLockTableInitialized(createdTable)) {
			executor.comment("Initialize Database Lock Table");
			executor.execute(new InitializeDatabaseChangeLogLockTableStatement());
			database.commit();
		}

		if (executor.updatesDatabase() && (database instanceof DerbyDatabase)
				&& ((DerbyDatabase) database).supportsBooleanDataType()
				|| database.getClass().isAssignableFrom(DB2Database.class)
						&& ((DB2Database) database).supportsBooleanDataType()) {
			// check if the changelog table is of an old smallint vs. boolean format
			String lockTable = database.escapeTableName(database.getLiquibaseCatalogName(),
					database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName());
			Object obj = executor.queryForObject(
					new RawSqlStatement("SELECT MIN(locked) AS test FROM " + lockTable + " FETCH FIRST ROW ONLY"),
					Object.class);
			if (!(obj instanceof Boolean)) { // wrong type, need to recreate table
				executor.execute(new DropTableStatement(database.getLiquibaseCatalogName(),
						database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName(), false));
				executor.execute(new CreateDatabaseChangeLogLockTableStatement());
				executor.execute(new InitializeDatabaseChangeLogLockTableStatement());
			}
		}

	}

}
