package liquibase.ext.cassandra.sqlgenerator;

import java.net.URI;

import liquibase.database.Database;

public class CassandraUtil {

	public static String getKeyspace(Database database) {
		
		return database.getDefaultSchemaName();

	}

}
