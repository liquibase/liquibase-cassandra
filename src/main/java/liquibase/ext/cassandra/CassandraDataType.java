package liquibase.ext.cassandra;

import liquibase.common.datatype.DataTypeWrapper;
import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.util.StringUtil;

public class CassandraDataType extends DataTypeWrapper {

	public CassandraDataType(LiquibaseDataType originalType) {
		super(extractOriginalType(originalType));
	}

	private static LiquibaseDataType extractOriginalType(LiquibaseDataType originalType) {
		return originalType;
	}

	@Override
	public DatabaseDataType toDatabaseDataType(Database database) {
		String originalDefinition = StringUtil.trimToEmpty(getRawDefinition());
		return super.toDatabaseDataType(database);
	}
}
