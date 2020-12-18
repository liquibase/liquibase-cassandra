package liquibase.ext.cassandra.sqlgenerator;

import java.net.URI;

import liquibase.database.Database;

public class CassandraUtil {

	public static String getKeyspace(Database database) {
		String conn = database.getConnection().getURL();
		String cleanURI = conn.substring(5);
		URI uri = URI.create(cleanURI);
		String keyspace = uri.getPath();
		keyspace = keyspace.substring(1); // remove the slash
		keyspace = keyspace.split(";")[0]; // remove arguments in the conn string
		keyspace = keyspace.split("\\?")[0]; // remove arguments in the conn string

		return keyspace;
	}

}
