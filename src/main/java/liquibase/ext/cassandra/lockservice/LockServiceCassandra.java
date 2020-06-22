package liquibase.ext.cassandra.lockservice;

import liquibase.database.Database;
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
import liquibase.statement.core.RawSqlStatement;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LockServiceCassandra extends StandardLockService {

	
	private boolean isDatabaseChangeLogLockTableInitialized;
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
		// TODO Auto-generated method stub
		return super.acquireLock();
		
		
		// are you locked
		
			// yes? throw error cause someone else is here
		
			// no? lock 
	}
	
	@Override
	public void releaseLock() throws LockException {
		// TODO Auto-generated method stub
		super.releaseLock();
	}

	@Override
    public boolean hasDatabaseChangeLogLockTable() throws DatabaseException {
        boolean hasChangeLogLockTable;
        try {
            Statement statement = ((CassandraDatabase) database).getStatement();
            statement.executeQuery("DESCRIBE " + CassandraUtil.getKeyspace(database) + ".DATABASECHANGELOGLOCK");
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
