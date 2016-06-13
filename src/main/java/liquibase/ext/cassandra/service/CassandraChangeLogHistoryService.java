package liquibase.ext.cassandra.service;

import liquibase.change.ColumnConfig;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class CassandraChangeLogHistoryService extends StandardChangeLogHistoryService {

    private Logger logger = LogFactory.getInstance().getLog("CassandraChangeLogHistoryService");

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof CassandraDatabase;
    }

    @Override
    public List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
        SelectFromDatabaseChangeLogStatement select = new SelectFromDatabaseChangeLogStatement(new ColumnConfig().setName("*").setComputed(true));
        return ExecutorService.getInstance().getExecutor(database).queryForList(select);
    }

    @Override
    public boolean hasDatabaseChangeLogTable() throws DatabaseException {
        boolean hasChangeLogTable;
        try {
            Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
            statement.executeQuery("select ID from DATABASECHANGELOG");
            statement.close();
            hasChangeLogTable = true;
        } catch (SQLException e) {
            logger.info("No DATABASECHANGELOG available in cassandra.");
            hasChangeLogTable = false;
        } catch (ClassNotFoundException e) {
            logger.warning(e.getMessage(),e);
            hasChangeLogTable = false;
        }
        // needs to be generated up front
        return hasChangeLogTable;
    }

}
