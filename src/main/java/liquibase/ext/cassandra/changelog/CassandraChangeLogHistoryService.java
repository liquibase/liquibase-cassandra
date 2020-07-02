package liquibase.ext.cassandra.changelog;

<<<<<<< HEAD
import liquibase.Scope;
=======
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

>>>>>>> origin/main
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.sqlgenerator.CassandraUtil;
import liquibase.logging.LogFactory;
import liquibase.statement.core.RawSqlStatement;

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
        RawSqlStatement select = new RawSqlStatement("SELECT * FROM " + CassandraUtil.getKeyspace(getDatabase())+ ".DATABASECHANGELOG");
        return ExecutorService.getInstance().getExecutor(database).queryForList(select);
    }    
    
    
}
