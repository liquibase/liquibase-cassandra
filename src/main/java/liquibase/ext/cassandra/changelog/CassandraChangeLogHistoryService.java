package liquibase.ext.cassandra.changelog;

import liquibase.ChecksumVersion;
import liquibase.Scope;
import liquibase.change.ColumnConfig;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.executor.jvm.ChangelogJdbcMdcListener;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

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
                    "FROM " + getChangeLogTableName());
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

    @Override
    public List<Map<String, ?>> getIncompatibleDatabaseChangeLogs() throws DatabaseException {
        /* Override default behavior to ensure compatibility with Cassandra CQL (see issue #375):
        re-implement the logic of the overridden method by selecting all the checksum values in the table
        DATABASECHANGELOG, then filtering the results in Java to implement the original WHERE clause
        'ByCheckSumNotNullAndNotLike'.
        */
        final SqlStatement databaseChangeLogStatement = new SelectFromDatabaseChangeLogStatement(
                new ColumnConfig().setName("MD5SUM")
        );
        final List<Map<String, ?>> allChecksums = ChangelogJdbcMdcListener.query(
                getDatabase(), ex -> ex.queryForList(databaseChangeLogStatement)
        );
        return allChecksums.stream().filter(resultItem -> {
            final var checksumValue = resultItem.get("md5sum");
            return checksumValue != null
                    && !checksumValue.toString().startsWith(format("%d:", ChecksumVersion.latest().getVersion()));
        }).toList();
    }

    private String getChangeLogTableName() {
        if (getDatabase().getLiquibaseCatalogName() != null) {
            return getDatabase().getLiquibaseCatalogName() + "." + getDatabase().getDatabaseChangeLogTableName();
        } else {
            return getDatabase().getDatabaseChangeLogTableName();
        }
    }
}
