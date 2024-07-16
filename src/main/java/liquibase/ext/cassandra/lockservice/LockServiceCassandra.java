package liquibase.ext.cassandra.lockservice;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.LoggingExecutor;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.lockservice.StandardLockService;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;
import liquibase.util.NetUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static liquibase.ext.cassandra.database.CassandraDatabase.isAwsKeyspacesCompatibilityModeEnabled;

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
    public boolean isDatabaseChangeLogLockTableInitialized(final boolean tableJustCreated, final boolean forceRecheck) {
        if (!isDatabaseChangeLogLockTableInitialized || forceRecheck) {
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

            // For AWS Keyspaces, it could be necessary to wait the table is ready in all the nodes of the cluster
            // before querying it.
            if (isAwsKeyspacesCompatibilityModeEnabled()) {
                if (!waitForDatabaseChangeLogLockTableReady(executor)) {
                    throw new UnexpectedLiquibaseException(
                            "Waiting for databaseChangeLogLock table ready failed or timed out");
                }
            }

            try {
                isDatabaseChangeLogLockTableInitialized = executeCountQuery(executor,
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
        return executeCountQuery(executor,
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

    /**
     * Execute a count query using an alternative if the AWS Keyspaces compatibility mode is enabled.
     *
     * @implNote Since aggregate functions like COUNT are not supported by AWS Keyspaces (see
     * <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html#cassandra-functions">
     * Cassandra functions in AWS Keyspaces</a>), this method tries to execute the same query without the COUNT
     * function then programmatically count returned rows, when the AWS Keyspaces compatibility mode is enabled.
     *
     * @param executor The query executor.
     * @param query    The query to execute.
     * @return The result of the count query.
     * @throws DatabaseException in case something goes wrong during the query execution or if the provided query is
     * not a count query.
     */
    private int executeCountQuery(final Executor executor, final String query) throws DatabaseException {
        if (!query.contains("SELECT COUNT(*)")) {
            throw new UnexpectedLiquibaseException("Invalid count query: " + query);
        }
        if (isAwsKeyspacesCompatibilityModeEnabled()) {
            Scope.getCurrentScope().getLog(LockServiceCassandra.class)
                    .fine("AWS Keyspaces compatibility mode enabled: using alternative count query");
            final String altQuery = query.replaceAll("(?i)SELECT COUNT\\(\\*\\)", "SELECT *");
            final List<Map<String, ?>> rows = executor.queryForList(new RawSqlStatement(altQuery));
            return rows.size();
        } else {
            return executor.queryForInt(new RawSqlStatement(query));
        }
    }

    /**
     * Execute a query on the AWS Keyspaces {@code tables} table to check if the database changelog lock table is
     * ready for querying (status {@code ACTIVE}).
     * <p>
     *     See: <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-tables.html#tables-create">
     *         AWS documentation about creating tables in Keyspaces</a>.
     * </p>
     *
     * @param executor The query executor.
     * @return {@code true} if the table is ready before reaching the maximal number of attempts, {@code false}
     * otherwise.
     */
    private boolean waitForDatabaseChangeLogLockTableReady(final Executor executor) {
        int maxAttempts = 20;
        final String tableName = database.getDatabaseChangeLogLockTableName();
        if (executor instanceof LoggingExecutor) {
            return true;
        }
        boolean isTableActive;
        try {
            int attempt = 1;
            do {
                try {
                    Thread.sleep(getChangeLogLockRecheckTime() * 2000);
                } catch (InterruptedException e) {
                    // Restore thread interrupt status
                    Thread.currentThread().interrupt();
                }
                Scope.getCurrentScope().getLog(LockServiceCassandra.class)
                        .fine("Checking the status of table " + tableName + " (attempt #" + attempt + ")...");
                attempt ++;
                final String tableStatusSqlStatement = "SELECT status FROM system_schema_mcs.tables "
                        + "WHERE keyspace_name = '" + database.getLiquibaseCatalogName() + "'"
                        + "AND table_name = " + tableName + "'";
                String status = executor.queryForObject(new RawSqlStatement(tableStatusSqlStatement), String.class);
                isTableActive = "ACTIVE".equalsIgnoreCase(status);
            } while (!isTableActive && attempt <= maxAttempts);
        } catch (final DatabaseException e) {
            Scope.getCurrentScope().getLog(LockServiceCassandra.class)
                .warning("Failed to check the status of table " + tableName + " in AWS Keyspaces");
            return false;
        }
        return isTableActive;
    }
}
