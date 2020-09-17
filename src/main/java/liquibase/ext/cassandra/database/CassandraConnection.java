package liquibase.ext.cassandra.database;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Driver;
import java.util.Properties;

import com.datastax.oss.driver.api.core.CqlSession;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;

public class CassandraConnection implements DatabaseConnection {

    private CqlSession db;
    

	
	@Override
	public int getPriority() {
		return PRIORITY_DEFAULT + 500;
	}

	@Override
	public void open(String url, Driver driverObject, Properties driverProperties) throws DatabaseException {
		// TODO Auto-generated method stub
		
//        this.connectionString = new ConnectionString(url);
//        this.con = MongoClients.create(this.connectionString);
//        this.db = this.con.getDatabase(Objects.requireNonNull(this.connectionString.getDatabase()))
//                .withCodecRegistry(uuidCodecRegistry());
//        
	
		URI myUri;
			try {
				String cleanUri = url.substring(5); //strip off 'jdbc:'
				myUri = new URI(cleanUri);
			} catch (URISyntaxException e) {
				throw new UnexpectedLiquibaseException(e);
			}
        
        this.db = CqlSession.builder()
        	    .addContactPoint(new InetSocketAddress(myUri.getHost(), myUri.getPort()))
        	    .withLocalDatacenter("dc1")
        	    .build();
		
		
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
