package liquibase.ext.cassandra.changelog;

import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.logging.LogFactory;

import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
            statement.executeQuery("select ID from DATABASECHANGELOG");
            statement.close();
            hasChangeLogTable = true;
        } catch (SQLException e) {
            LogFactory.getLogger().info("No DATABASECHANGELOG available in cassandra.");
            hasChangeLogTable = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            hasChangeLogTable = false;
        }

        // needs to be generated up front
        return hasChangeLogTable;
    }


//    @Override
//    public int getNextSequenceValue() throws LiquibaseException {
//        int next = 0;
//        try {
//            Statement statement = ((CassandraDatabase) getDatabase()).getStatement();
//            // get the keyspace from the url
//            String conn = getDatabase().getConnection().getURL();
//            String cleanURI = conn.substring(5);
//            URI uri = URI.create(cleanURI);
//            String keyspace = uri.getPath();
//            keyspace = keyspace.substring(1); // remove the slash
//            keyspace = keyspace.split(";")[0]; // remove arguments in the conn string
//            keyspace = keyspace.split("\\?")[0]; // remove arguments in the conn string
//            
//            
//            ResultSet rs = statement.executeQuery("SELECT KEY, AUTHOR, ORDEREXECUTED FROM " + keyspace + ".DATABASECHANGELOGLOCK");
//            while (rs.next()) {
//                int order = rs.getInt("ORDEREXECUTED");
//                next = Math.max(order, next);
//            }
//            statement.close();
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        return next + 1;
//    }
}
