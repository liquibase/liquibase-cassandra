package liquibase.ext.cassandra.changelog;

import liquibase.Scope;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.sqlgenerator.CassandraUtil;
import liquibase.statement.core.RawSqlStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;

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
            CqlSession session = ((CassandraDatabase) getDatabase()).getSession();
            ResultSet rs = (ResultSet) session.execute("select ID from " + CassandraUtil.getKeyspace(getDatabase()) + ".DATABASECHANGELOG");
            if (rs.next() == true) {
            	hasChangeLogTable = true;
            } else {
            	hasChangeLogTable = false;
            }
            session.close();
        } catch (SQLException e) {
            Scope.getCurrentScope().getLog(getClass()).info("No DATABASECHANGELOG available in cassandra.");
            hasChangeLogTable = false;
        }

        // needs to be generated up front
        return hasChangeLogTable;
    }


    @Override
    public int getNextSequenceValue() throws LiquibaseException {
        int next = 0;
        try {
            CqlSession session = ((CassandraDatabase) getDatabase()).getSession();
            ResultSet rs = (ResultSet) session.execute("SELECT ID, AUTHOR, ORDEREXECUTED FROM " + CassandraUtil.getKeyspace(getDatabase()) + ".DATABASECHANGELOG");
            while (rs.next()) {
                int order = rs.getInt("ORDEREXECUTED");
                next = Math.max(order, next);
            }
            session.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return next + 1;
    }

    @Override
    public List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
        RawSqlStatement select = new RawSqlStatement("SELECT * FROM " + CassandraUtil.getKeyspace(getDatabase()) + ".DATABASECHANGELOG");
        return Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).queryForList(select);
    }


}
