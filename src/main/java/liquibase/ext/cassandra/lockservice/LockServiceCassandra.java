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
                if ((rowsUpdated == -1) && (database instanceof MSSQLDatabase)) {

                    Scope.getCurrentScope().getLog(this.getClass()).info("Database did not return a proper row count (Might have NOCOUNT enabled)");
                    database.rollback();
                    Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(
                            new LockDatabaseChangeLogStatement(), database
                    );
                    if (sql.length != 1) {
                        throw new UnexpectedLiquibaseException("Did not expect " + sql.length + " statements");
                    }
                    rowsUpdated = executor.update(new RawSqlStatement("EXEC sp_executesql N'SET NOCOUNT OFF " +
                            sql[0].toSql().replace("'", "''") + "'"));
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
            if (this.hasDatabaseChangeLogLockTable()) {
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
    public boolean hasDatabaseChangeLogLockTable() {
        boolean hasChangeLogLockTable;
        try {
            Statement statement = ((CassandraDatabase) database).getStatement();
            statement.executeQuery("SELECT ID from " + getChangeLogLockTableName());
            statement.close();
            hasChangeLogLockTable = true;
        } catch (SQLException e) {
            Scope.getCurrentScope().getLog(getClass()).info("No " + getChangeLogLockTableName() + " available in cassandra.");
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
                isDatabaseChangeLogLockTableInitialized = executor.queryForInt(
                        new RawSqlStatement("SELECT COUNT(*) FROM " + getChangeLogLockTableName())
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

    private boolean isLocked(Executor executor) throws DatabaseException {
        // Check to see if current process holds the lock each time
        return isLockedByCurrentInstance(executor);
    }

    private boolean isLockedByCurrentInstance(Executor executor) throws DatabaseException {
        final String lockedBy = NetUtil.getLocalHostName() + " (" + NetUtil.getLocalHostAddress() + ")";
        return executor.queryForInt(
                new RawSqlStatement("SELECT COUNT(*) FROM " + getChangeLogLockTableName() + " where " +
                        "LOCKED = TRUE AND LOCKEDBY = '" + lockedBy + "' ALLOW FILTERING")
        ) > 0;
    }

    private String getChangeLogLockTableName() {
        if (database.getLiquibaseCatalogName() != null) {
            return database.getLiquibaseCatalogName() + "." + database.getDatabaseChangeLogLockTableName();
        } else {
            return database.getDatabaseChangeLogLockTableName();
        }
    }
}
