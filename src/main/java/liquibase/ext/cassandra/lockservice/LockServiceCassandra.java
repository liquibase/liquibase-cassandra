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
import liquibase.lockservice.StandardLockService;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;
import liquibase.util.NetUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

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

        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

        try {

            database.rollback();
            super.init();


            if (isLocked(executor)) {
                return false;
            } else {

                executor.comment("Lock Database");
                int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());
                if (rowsUpdated == -1 && !isLockedByCurrentInstance(executor)) {
                    // another node was faster
                    return false;
                }
                if (rowsUpdated > 1) {
                    throw new LockException("Did not update change log lock correctly");
                }
                if (rowsUpdated == 0) {
                    // another node was faster
                    return false;
                }
                database.commit();
                Scope.getCurrentScope().getLog(this.getClass()).info("successfully.acquired.change.log.lock");


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
            if (this.isDatabaseChangeLogLockTableCreated()) {
                executor.comment("Release Database Lock");
                database.rollback();
                executor.update(new UnlockDatabaseChangeLogStatement());
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
    public boolean isDatabaseChangeLogLockTableCreated() {
        boolean hasChangeLogLockTable;
        try {
            Statement statement = ((CassandraDatabase) database).getStatement();
            statement.executeQuery("SELECT ID FROM " + getChangeLogLockTableName());
            statement.close();
            hasChangeLogLockTable = true;
        } catch (SQLException e) {
            Scope.getCurrentScope().getLog(getClass()).info("No " + getChangeLogLockTableName() + " available in Cassandra.");
            hasChangeLogLockTable = false;
        } catch (DatabaseException e) {
            e.printStackTrace();
            hasChangeLogLockTable = false;
        }

        // needs to be generated up front
        return hasChangeLogLockTable;
    }

    @Override
    public boolean isDatabaseChangeLogLockTableInitialized(final boolean tableJustCreated) {
        if (!isDatabaseChangeLogLockTableInitialized) {
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

            try {
                isDatabaseChangeLogLockTableInitialized =  executeCountQueryWithAlternative(executor,
                        "SELECT COUNT(*) FROM " + getChangeLogLockTableName()) > 0;
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

    private boolean isLocked(Executor executor) throws DatabaseException {
        // Check to see if current process holds the lock each time
        return isLockedByCurrentInstance(executor);
    }

    private boolean isLockedByCurrentInstance(Executor executor) throws DatabaseException {
        final String lockedBy = NetUtil.getLocalHostName() + " (" + NetUtil.getLocalHostAddress() + ")";
        return executeCountQueryWithAlternative(executor,
                "SELECT COUNT(*) FROM " + getChangeLogLockTableName() + " WHERE " +
                        "LOCKED = TRUE AND LOCKEDBY = '" + lockedBy + "' ALLOW FILTERING") > 0;
    }

    private String getChangeLogLockTableName() {
        if (database.getLiquibaseCatalogName() != null) {
            return database.getLiquibaseCatalogName() + "." + database.getDatabaseChangeLogLockTableName();
        } else {
            return database.getDatabaseChangeLogLockTableName();
        }
    }

    private int executeCountQueryWithAlternative(final Executor executor, final String query) throws DatabaseException {
        if (!query.contains("SELECT COUNT(*)")) {
            throw new UnexpectedLiquibaseException("Invalid count query: " + query);
        }
        try {
            return executor.queryForInt(new RawSqlStatement(query));
        } catch (DatabaseException e) {
            // If the count query failed (for example, because counting rows is not implemented - see issue #289 with
            // AWS Keyspaces where aggregate functions like COUNT are not supported:
            // https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html#cassandra-functions), try to
            // execute the same query without the COUNT function then programmatically count returned rows.
            final String altQuery = query.replace("SELECT COUNT(*)", "SELECT *");
            final List<Map<String, ?>> rows = executor.queryForList(new RawSqlStatement(altQuery));
            return rows.size();
        }
    }
}
