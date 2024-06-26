package liquibase.ext.cassandra.sqlgenerator;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.lockservice.LockServiceCassandra;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DeleteGenerator;
import liquibase.statement.core.DeleteStatement;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Table;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.cassandra.database.CassandraDatabase.isAwsKeyspacesCompatibilityModeEnabled;

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
			if (isAwsKeyspacesCompatibilityModeEnabled()) {
				Scope.getCurrentScope().getLog(DeleteGeneratorCassandra.class)
						.fine("AWS Keyspaces compatibility mode enabled: using alternative queries to truncate " + statement.getTableName());
				return buildDeleteStatements(statement, database, sqlGeneratorChain).toArray(new Sql[]{});
			} else {
				String sql = "TRUNCATE " + database.escapeTableName(statement.getCatalogName(),
						statement.getSchemaName(), statement.getTableName());
				return new Sql[] { new UnparsedSql(sql, getAffectedTable(statement)) };
			}
		} else {
			return super.generateSql(statement, database, sqlGeneratorChain);
		}

	}

	/**
	 * Builds a list of DELETE statements to remove each row of a given table.
	 *
	 * @implNote Since AWS Keyspaces does not support TRUNCATE TABLE statements (see
	 * <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html#cassandra-api-support">
	 * Cassandra APIs in AWS Keyspaces</a>), this method tries to build a DELETE statement for each row in the table
	 * to truncate, when the AWS Keyspaces compatibility mode is enabled.
	 * Here, we cannot simply drop then re-create the table because the delete generator is too generic, so we can't
	 * easily know the structure of the table to re-create.
	 *
	 * @param statement 		The original DELETE statement not having any WHERE clause.
	 * @param database  		The database where the queries will be executed.
	 * @param sqlGeneratorChain The SQL generator chain.
	 * @return The list of DELETE statements to execute to truncate the given table.
	 * @throws UnexpectedLiquibaseException in case something goes wrong during the preparation of the statements.
	 */
	private List<Sql> buildDeleteStatements(final DeleteStatement statement, final Database database,
											final SqlGeneratorChain sqlGeneratorChain) {
		List<Sql> sqlStatements = new ArrayList<>();

		String tableName = database.escapeTableName(statement.getCatalogName(),
				statement.getSchemaName(), statement.getTableName());

		// Get the primary key values of each row in the table.
		Table table = (Table) getAffectedTable(statement);
		PrimaryKey pk = table.getPrimaryKey();
		String pkColumnNames = pk.getColumnNames();
		String selectTableContent = "SELECT " + pkColumnNames + " FROM " + tableName;

		// For each row in the table, build a DELETE statement for which the WHERE clause is based on the primary key
		// of the row.
		try {
			Statement stmt = ((CassandraDatabase) database).getStatement();
			ResultSet rs = stmt.executeQuery(selectTableContent);
			while (rs.next()) {
				DeleteStatement deleteRowStatement = new DeleteStatement(statement.getCatalogName(),
						statement.getSchemaName(), statement.getTableName());
				statement.addWhereColumnName(pkColumnNames);
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					statement.addWhereParameter(rs.getObject(i));
				}
				sqlStatements.add(super.generateSql(deleteRowStatement, database, sqlGeneratorChain)[0]);
			}
			rs.close();
			stmt.close();
		} catch (DatabaseException | SQLException e) {
			throw new UnexpectedLiquibaseException(
					"Failed to build DELETE statements to truncate table " + tableName, e);
		}
		return sqlStatements;
	}
}
