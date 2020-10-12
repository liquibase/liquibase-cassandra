package liquibase.ext.cassandra.changelog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import liquibase.Scope;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.sqlgenerator.CassandraUtil;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.UpdateStatement;

public class CassandraChangeLogHistoryService extends StandardChangeLogHistoryService {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof CassandraDatabase;
    }

    @Override
    public boolean hasDatabaseChangeLogTable() {
        boolean hasChangeLogTable;
        try {
            Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
            statement.executeQuery("select ID from " + CassandraUtil.getKeyspace(getDatabase()) + ".DATABASECHANGELOG");
            statement.close();
            hasChangeLogTable = true;
        } catch (SQLException e) {
            Scope.getCurrentScope().getLog(getClass()).info("No DATABASECHANGELOG available in cassandra.");
            hasChangeLogTable = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            hasChangeLogTable = false;
        }

        // needs to be generated up front
        return hasChangeLogTable;
    }


    @Override
    public int getNextSequenceValue() throws LiquibaseException {
        int next = 0;
        try {
            Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
            ResultSet rs = statement.executeQuery("SELECT ID, AUTHOR, ORDEREXECUTED FROM " + CassandraUtil.getKeyspace(getDatabase()) + ".DATABASECHANGELOG");
            while (rs.next()) {
                int order = rs.getInt("ORDEREXECUTED");
                next = Math.max(order, next);
            }
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return next + 1;
    }

    @Override
    public List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
        RawSqlStatement select = new RawSqlStatement("SELECT * FROM " + CassandraUtil.getKeyspace(getDatabase()) + ".DATABASECHANGELOG");
        final List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).queryForList(select);
        returnList.sort(Comparator.comparing((Map<String, ?> o) -> (Date) o.get("DATEEXECUTED")).thenComparingInt(o -> (Integer) o.get("ORDEREXECUTED")));
        return returnList;
    }

    @Override
    public void clearAllCheckSums() throws LiquibaseException {
        final List<Map<String, ?>> returnList = this.queryDatabaseChangeLogTable(getDatabase());
        // interate list


            // UPDATE databasechangelog set md5sum = null where id ='1';

        Database database = getDatabase();
        UpdateStatement updateStatement = new UpdateStatement(database.getLiquibaseCatalogName(), database
                .getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName());
        updateStatement.addNewColumnValue("MD5SUM", null);
        Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).execute(updateStatement);
        database.commit();
    }
}
