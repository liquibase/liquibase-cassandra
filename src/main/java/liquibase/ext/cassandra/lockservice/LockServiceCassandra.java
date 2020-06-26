package liquibase.ext.cassandra.lockservice;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
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
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LockServiceCassandra extends StandardLockService {

	
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
		
        Executor executor = ExecutorService.getInstance().getExecutor(database);

        try {

        	database.rollback();
	        super.init();

	        
	        	Boolean locked = executor.queryForInt(
	        			new RawSqlStatement("SELECT COUNT(*) FROM " + CassandraUtil.getKeyspace(database) + ".DATABASECHANGELOGLOCK where locked = TRUE ALLOW FILTERING")
	        			) > 0;

	            if (locked) {
	                return false;
	            } else {

	                executor.comment("Lock Database");
	                int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());
	                if ((rowsUpdated == -1) && (database instanceof MSSQLDatabase)) {
	                	
	                	LogFactory.getLogger().info("Database did not return a proper row count (Might have NOCOUNT enabled)");
	                    database.rollback();
	                    Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(
	                            new LockDatabaseChangeLogStatement(), database
	                    );
	                    if (sql.length != 1) {
	                        throw new UnexpectedLiquibaseException("Did not expect "+sql.length+" statements");
	                    }
	                    rowsUpdated = executor.update(new RawSqlStatement("EXEC sp_executesql N'SET NOCOUNT OFF " +
	                            sql[0].toSql().replace("'", "''") + "'"));
	                }
	                if (rowsUpdated > 1) {
	                    throw new LockException("Did not update change log lock correctly");
	                }
	                if (rowsUpdated == 0)
	                {
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

        Executor executor = ExecutorService.getInstance().getExecutor(database);
        try {
            if (this.hasDatabaseChangeLogLockTable()) {
                executor.comment("Release Database Lock");
                database.rollback();
                int updatedRows = executor.update(new UnlockDatabaseChangeLogStatement());
                if ((updatedRows == -1) && (database instanceof MSSQLDatabase)) {
                	LogFactory.getLogger().info("Database did not return a proper row count (Might have NOCOUNT enabled.)");
                    database.rollback();
                    Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(
                            new UnlockDatabaseChangeLogStatement(), database
                    );
                    if (sql.length != 1) {
                        throw new UnexpectedLiquibaseException("Did not expect "+sql.length+" statements");
                    }
                    updatedRows = executor.update(
                            new RawSqlStatement(
                                    "EXEC sp_executesql N'SET NOCOUNT OFF " +
                                            sql[0].toSql().replace("'", "''") + "'"
                            )
                    );
                }
                if (updatedRows != 1) {
                    throw new LockException(
                            "Did not update change log lock correctly.\n\n" +
                                    updatedRows +
                                    " rows were updated instead of the expected 1 row using executor " +
                                    executor.getClass().getName() + "" +
                                    " there are " +
                                    executor.queryForInt(
                                            new RawSqlStatement(
                                                    "SELECT COUNT(*) FROM " +
                                                            database.getDatabaseChangeLogLockTableName()
                                            )
                                    ) +
                                    " rows in the table"
                    );
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
            statement.executeQuery("SELECT ID from " + CassandraUtil.getKeyspace(database) + ".DATABASECHANGELOGLOCK");
            statement.close();
            hasChangeLogLockTable = true;
        } catch (SQLException e) {
            LogFactory.getLogger().info("No DATABASECHANGELOGLOCK available in cassandra.");
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
            Executor executor = ExecutorService.getInstance().getExecutor(database);

			try {
                isDatabaseChangeLogLockTableInitialized = executor.queryForInt(
               		new RawSqlStatement("SELECT COUNT(*) FROM " + CassandraUtil.getKeyspace(database) + ".DATABASECHANGELOGLOCK")
                ) > 0;
            } catch (LiquibaseException e) {
                if (executor.updatesDatabase()) {
                    throw new UnexpectedLiquibaseException(e);
                } else {
                    //probably didn't actually create the table yet.
                    isDatabaseChangeLogLockTableInitialized = !tableJustCreated;
                }
            }
        }
        return isDatabaseChangeLogLockTableInitialized;
    }	

}
