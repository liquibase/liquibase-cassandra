package liquibase.ext.cassandra.changelog;

import liquibase.Scope;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.core.RawSqlStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class CassandraChangeLogHistoryService extends StandardChangeLogHistoryService {

    @Override
    public int getPriority() {
        return PrioritizedService.PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof CassandraDatabase;
    }

    @Override
    public boolean hasDatabaseChangeLogTable() {
        boolean hasChangeLogTable;
        try (Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
             ResultSet rs = statement.executeQuery("select ID from " + getChangeLogTableName())) {
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
        try (Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
             ResultSet rs = statement.executeQuery("SELECT " +
                     "ID as \"ID\", " +
                     "AUTHOR as \"AUTHOR\", " +
                     "FILENAME as \"FILENAME\", " +
                     "COMMENTS AS \"COMMENTS\", " +
                     "CONTEXTS AS \"CONTEXTS\", " +
                     "DATEEXECUTED AS \"DATEEXECUTED\", " +
                     "ORDEREXECUTED AS \"ORDEREXECUTED\", " +
                     "DEPLOYMENT_ID AS \"DEPLOYMENT_ID\", " +
                     "DESCRIPTION AS \"DESCRIPTION\", " +
                     "EXECTYPE AS \"EXECTYPE\", " +
                     "LABELS AS \"LABELS\", " +
                     "LIQUIBASE AS \"LIQUIBASE\", " +
                     "MD5SUM AS \"MD5SUM\", " +
                     "TAG AS \"TAG\" " +
                     "FROM " + getChangeLogTableName())) {
            while (rs.next()) {
                int order = rs.getInt("ORDEREXECUTED");
                next = Math.max(order, next);
            }
        } catch (SQLException | DatabaseException e) {
            e.printStackTrace();
        }
        return next + 1;
    }

    @Override
    public List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
        if (!hasDatabaseChangeLogTable()) {
            return Collections.emptyList();
        }
        RawSqlStatement select = new RawSqlStatement("SELECT " +
                "ID as \"ID\", " +
                "AUTHOR as \"AUTHOR\", " +
                "FILENAME as \"FILENAME\", " +
                "COMMENTS AS \"COMMENTS\", " +
                "CONTEXTS AS \"CONTEXTS\", " +
                "DATEEXECUTED AS \"DATEEXECUTED\", " +
                "ORDEREXECUTED AS \"ORDEREXECUTED\", " +
                "DEPLOYMENT_ID AS \"DEPLOYMENT_ID\", " +
                "DESCRIPTION AS \"DESCRIPTION\", " +
                "EXECTYPE AS \"EXECTYPE\", " +
                "LABELS AS \"LABELS\", " +
                "LIQUIBASE AS \"LIQUIBASE\", " +
                "MD5SUM AS \"MD5SUM\", " +
                "TAG AS \"TAG\" " +
                "FROM " + getChangeLogTableName());
        final List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).queryForList(select);
        returnList.sort(Comparator.comparing((Map<String, ?> o) -> (Date) o.get("DATEEXECUTED")).thenComparingInt(o -> (Integer) o.get("ORDEREXECUTED")));
        return returnList;
    }

    /**
     * Override tagExists to avoid a WHERE clause on the TAG column, which is not part of the
     * Cassandra primary key (ID, AUTHOR, FILENAME). A bare {@code WHERE TAG='...'} query would
     * fail with "Cannot execute this query … use ALLOW FILTERING", and the COUNT(*) workaround
     * is not supported by AWS Keyspaces. We reuse the already-correct full-table-scan from
     * {@link #queryDatabaseChangeLogTable} and filter in Java instead.
     */
    @Override
    public boolean tagExists(String tag) throws DatabaseException {
        if (!hasDatabaseChangeLogTable()) {
            return false;
        }
        List<Map<String, ?>> rows = queryDatabaseChangeLogTable(getDatabase());
        return rows.stream().anyMatch(row -> tag.equals(row.get("TAG")));
    }

    private String getChangeLogTableName() {
        if (getDatabase().getLiquibaseCatalogName() != null) {
            return getDatabase().getLiquibaseCatalogName() + "." + getDatabase().getDatabaseChangeLogTableName();
        } else {
            return getDatabase().getDatabaseChangeLogTableName();
        }
    }
}
