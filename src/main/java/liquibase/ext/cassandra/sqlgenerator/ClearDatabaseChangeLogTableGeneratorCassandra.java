package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ClearDatabaseChangeLogTableGenerator;
import liquibase.statement.core.ClearDatabaseChangeLogTableStatement;
import liquibase.util.StringUtil;

public class ClearDatabaseChangeLogTableGeneratorCassandra extends ClearDatabaseChangeLogTableGenerator {

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public Sql[] generateSql(ClearDatabaseChangeLogTableStatement statement, Database database,
			SqlGeneratorChain sqlGeneratorChain) {

		String schemaName;
		if (StringUtil.isNotEmpty(statement.getSchemaName())) {
			schemaName = statement.getSchemaName();
		} else {
			schemaName = database.getLiquibaseSchemaName();
		}

		return new Sql[] { new UnparsedSql("TRUNCATE " + database.escapeTableName(database.getLiquibaseCatalogName(),
				schemaName, database.getDatabaseChangeLogTableName()), getAffectedTable(database, schemaName)) };

	}

}
