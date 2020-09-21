package liquibase.ext.cassandra.database;

import java.sql.Driver;
import java.util.Properties;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

public class CassandraConnection implements DatabaseConnection {

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public void open(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws DatabaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() throws DatabaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean getAutoCommit() throws DatabaseException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getCatalog() throws DatabaseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String nativeSQL(String sql) throws DatabaseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void rollback() throws DatabaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws DatabaseException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getDatabaseProductName() throws DatabaseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDatabaseProductVersion() throws DatabaseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getDatabaseMajorVersion() throws DatabaseException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws DatabaseException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getURL() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getConnectionUserName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isClosed() throws DatabaseException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void attached(Database database) {
		// TODO Auto-generated method stub
		
	}


}