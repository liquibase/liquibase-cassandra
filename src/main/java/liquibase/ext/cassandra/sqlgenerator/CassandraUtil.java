package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;

public class CassandraUtil {

	public static String getKeyspace(Database database) {
		return database.getDefaultSchemaName();
	}

}
