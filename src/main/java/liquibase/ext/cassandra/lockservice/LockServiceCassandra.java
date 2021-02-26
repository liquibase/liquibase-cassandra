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
import liquibase.logging.LogFactory;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

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


            boolean locked = executor.queryForInt(
                    new RawSqlStatement("SELECT COUNT(*) FROM " + database.getDefaultCatalogName() + ".DATABASECHANGELOGLOCK where " +
                            "locked = TRUE ALLOW FILTERING")
            ) > 0;

            if (locked) {
                return false;
            } else {

                executor.comment("Lock Database");
                int rowsUpdated = executor.update(new LockDatabaseChangeLogStatement());
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
    public void init() throws DatabaseException {
        super.init();

        // table creation in AWS Keyspaces is not immediate like other Cassandras
        // https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-tables.html#tables-create
        // let's see if the DATABASECHANGELOG table is active before doing stuff

        int DBCL_TABLE_ACTIVE = 0;
        while (DBCL_TABLE_ACTIVE == 0) {

            try {
                Statement statement = ((CassandraDatabase) database).getStatement();
                ResultSet rs = statement.executeQuery("SELECT keyspace_name, table_name, status FROM " +
                        "system_schema_mcs.tables WHERE keyspace_name = '" + database.getDefaultCatalogName() +
                        "' AND table_name = 'DATABASECHANGELOGLOCK'");
                while (rs.next()) {
                    String status = rs.getString("status");
                    if (status.equals("ACTIVE")) {
                        DBCL_TABLE_ACTIVE = 1;
                        //table is active, we're done here
                    } else if (status.equals("CREATING")) {
                        TimeUnit.SECONDS.sleep(3);
                    } else {
                        // something went very wrong, are we having issues with another Cassandra platform...?
                    }

                }
            } catch (ClassNotFoundException e) {
                throw new DatabaseException(e);
            } catch (InterruptedException e) {
                throw new DatabaseException(e);
            } catch (SQLException e) {
                throw new DatabaseException(e);
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
            statement.executeQuery("SELECT ID from " + database.getDefaultCatalogName() + ".DATABASECHANGELOGLOCK");
            statement.close();
            hasChangeLogLockTable = true;
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
    public boolean isDatabaseChangeLogLockTableInitialized(final boolean tableJustCreated) {
        if (!isDatabaseChangeLogLockTableInitialized) {
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);

            try {
                isDatabaseChangeLogLockTableInitialized = executor.queryForInt(
                        new RawSqlStatement("SELECT COUNT(*) FROM " + database.getDefaultCatalogName() + ".DATABASECHANGELOGLOCK")
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
