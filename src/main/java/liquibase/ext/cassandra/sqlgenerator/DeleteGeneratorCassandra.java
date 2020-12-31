package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DeleteGenerator;
import liquibase.statement.core.DeleteStatement;

public class DeleteGeneratorCassandra extends DeleteGenerator {

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supports(DeleteStatement statement, Database database) {
		return super.supports(statement, database) && database instanceof CassandraDatabase;
	}

	@Override
	public Sql[] generateSql(DeleteStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

		if (statement.getWhere() == null) {

			StringBuilder sql = new StringBuilder("TRUNCATE ").append(database
					.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));

			return new Sql[] { new UnparsedSql(sql.toString(), getAffectedTable(statement)) };
		} else {

			return super.generateSql(statement, database, sqlGeneratorChain);
		}

	}

}
