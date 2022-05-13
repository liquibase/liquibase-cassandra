package liquibase.ext.cassandra.changelog;

import liquibase.Scope;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.statement.core.RawSqlStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
            statement.executeQuery("select ID from " + getChangeLogTableName());
            statement.close();
            hasChangeLogTable = true;
        } catch (SQLException e) {
            Scope.getCurrentScope().getLog(getClass()).info("No " + getChangeLogTableName() + " available in cassandra.");
            hasChangeLogTable = false;
        } catch (DatabaseException e) {
            e.printStackTrace();
            hasChangeLogTable = false;
        }

        // needs to be generated up front
        return hasChangeLogTable;
    }


    @Override
    public int getNextSequenceValue() {
        int next = 0;
        try {
            Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
            ResultSet rs = statement.executeQuery("SELECT ID, AUTHOR, ORDEREXECUTED FROM " + getChangeLogTableName());
            while (rs.next()) {
                int order = rs.getInt("ORDEREXECUTED");
                next = Math.max(order, next);
            }
            statement.close();

        } catch (SQLException | DatabaseException e) {
            e.printStackTrace();
        }
        return next + 1;
    }

    @Override
    public List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
        RawSqlStatement select = new RawSqlStatement("SELECT * FROM " + getChangeLogTableName());
        final List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).queryForList(select);
        returnList.sort(Comparator.comparing((Map<String, ?> o) -> (Date) o.get("DATEEXECUTED")).thenComparingInt(o -> (Integer) o.get("ORDEREXECUTED")));
        return returnList;
    }

    private String getChangeLogTableName() {
        if (getDatabase().getLiquibaseCatalogName() != null) {
            return getDatabase().getLiquibaseCatalogName() + "." + getDatabase().getDatabaseChangeLogTableName();
        } else {
            return getDatabase().getDatabaseChangeLogTableName();
        }
    }
}
