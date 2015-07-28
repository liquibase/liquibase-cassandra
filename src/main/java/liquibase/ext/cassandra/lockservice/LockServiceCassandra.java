package liquibase.ext.cassandra.lockservice;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.lockservice.StandardLockService;
import liquibase.logging.LogFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LockServiceCassandra extends StandardLockService {

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
            statement.executeQuery("select ID from DATABASECHANGELOGLOCK");
            statement.close();
            hasChangeLogLockTable = true;
        } catch (SQLException e) {
            LogFactory.getLogger().info("No DATABASECHANGELOGLOCK available in cassandra.");
            hasChangeLogLockTable = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            hasChangeLogLockTable = false;
        }

        // needs to be generated up front
        return hasChangeLogLockTable;
    }

}
