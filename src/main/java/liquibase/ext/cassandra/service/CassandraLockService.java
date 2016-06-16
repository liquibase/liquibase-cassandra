package liquibase.ext.cassandra.service;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.database.CassandraQueries;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CassandraLockService extends StandardLockService {

    private Logger logger = LogFactory.getInstance().getLog("CassandraLockService");
    private boolean isDatabaseChangeLogLockTableInitialized;

    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    public boolean supports(Database database) {
        return database instanceof CassandraDatabase;
    }

    public boolean hasDatabaseChangeLogLockTable() throws DatabaseException {
        boolean hasChangeLogLockTable;
        try {
            Statement statement = ((CassandraDatabase) database).getStatement();
            ResultSet rs = statement.executeQuery(CassandraQueries.SELECT_ID_LOCKED_FROM_LIQUIBASE_DATABASECHANGELOGLOCK);
            while (rs.next()) {
                int id = rs.getInt("id");
                boolean locked = rs.getBoolean("locked");
                logger.info("change lock ID " + id + " locked " + locked);
            }
            statement.close();
            hasChangeLogLockTable = true;
        } catch (SQLException e) {
            logger.warning("No DATABASECHANGELOGLOCK available in cassandra.", e);
            hasChangeLogLockTable = false;
        } catch (ClassNotFoundException e) {
            logger.severe(e.getMessage(), e);
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
                //com.datastax.driver.core.exceptions.CodecNotFoundException: Codec not found for requested operation: [bigint <-> java.lang.Integer]
                isDatabaseChangeLogLockTableInitialized = executor.queryForLong(new RawSqlStatement("select count(*) from " + database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName()))) > 0;
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

    @Override
    public boolean acquireLock() throws LockException {
        if (hasChangeLogLock) {
            return true;
        }

        Executor executor = ExecutorService.getInstance().getExecutor(database);

        try {
            database.rollback();
            this.init();

            Boolean locked = (Boolean) ExecutorService.getInstance().getExecutor(database).queryForObject(new SelectFromDatabaseChangeLogLockStatement("LOCKED"), Boolean.class);

            if (locked) {
                return false;
            } else {
                executor.comment("Lock Database");
                executor.update(new LockDatabaseChangeLogStatement());
                if (!isLocked()) {
                    return false;
                }
                database.commit();
                logger.info("Successfully acquired change log lock");
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
                ;
            }
        }
    }

    private boolean isLocked() throws DatabaseException {
        return (Boolean) ExecutorService.getInstance().getExecutor(database).queryForObject(new SelectFromDatabaseChangeLogLockStatement("LOCKED"), Boolean.class);
    }

    @Override
    public void releaseLock() throws LockException {
        Executor executor = ExecutorService.getInstance().getExecutor(database);
        try {
            if (this.hasDatabaseChangeLogLockTable()) {
                executor.comment("Release Database Lock");
                database.rollback();
                executor.update(new UnlockDatabaseChangeLogStatement());
                if (isLocked()) {
                    throw new LockException("Did not update change log lock correctly the expected 1 row using executor " + executor.getClass().getName() + " there are " + executor.queryForLong(new RawSqlStatement("select count(*) from " + database.getDatabaseChangeLogLockTableName())) + " rows in the table");
                }
                database.commit();
            }
        } catch (Exception e) {
            throw new LockException(e);
        } finally {
            try {
                hasChangeLogLock = false;

                database.setCanCacheLiquibaseTableInfo(false);
                logger.info("Successfully released change log lock");
                database.rollback();
            } catch (DatabaseException e) {
                ;
            }
        }
    }


}
