package liquibase.ext.cassandra.sqlgenerator;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.lockservice.LockServiceCassandra;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.TagDatabaseGenerator;
import liquibase.statement.core.TagDatabaseStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static liquibase.ext.cassandra.database.CassandraDatabase.isAwsKeyspacesCompatibilityModeEnabled;

public class TagDatabaseGeneratorCassandra extends TagDatabaseGenerator {

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supports(TagDatabaseStatement statement, Database database) {
		return database instanceof CassandraDatabase;
	}

	@Override
	public Sql[] generateSql(TagDatabaseStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
		ObjectQuotingStrategy currentStrategy = database.getObjectQuotingStrategy();
		database.setObjectQuotingStrategy(ObjectQuotingStrategy.LEGACY);

		try {
			String tagEscaped = DataTypeFactory.getInstance().fromObject(statement.getTag(), database).objectToSql(statement.getTag(), database);

			Statement statement1 = ((CassandraDatabase) database).getStatement();
			String databaseChangelogTableName = database.escapeTableName(database.getLiquibaseCatalogName(),
					database.getLiquibaseSchemaName(), "databasechangelog");

			//Query to get last executed changeset date
			ResultSet rs1;
			PreparedStatement ps1 = null;
			// When AWS Keyspaces compatibility mode is enabled, the max date must be calculated programmatically.
			if (isAwsKeyspacesCompatibilityModeEnabled()) {
				Scope.getCurrentScope().getLog(LockServiceCassandra.class)
						.fine("AWS Keyspaces compatibility mode enabled: using alternative to get last executed changeset");
				Timestamp maxDateExecuted = selectLastExecutedChangesetTimestamp(database, databaseChangelogTableName);
				String query1 = "SELECT TOUNIXTIMESTAMP(DATEEXECUTED) as LAST_DATEEXECUTED FROM "
						+ databaseChangelogTableName + "WHERE DATEEXECUTED = ? ALLOW FILTERING;";
				ps1 = ((CassandraDatabase) database).prepareStatement(query1);
				ps1.setTimestamp(1, maxDateExecuted);
				rs1 = ps1.executeQuery();
			} else {
				String query1 = "SELECT TOUNIXTIMESTAMP(MAX(DATEEXECUTED)) as LAST_DATEEXECUTED FROM "
						+ databaseChangelogTableName;
				rs1 = statement1.executeQuery(query1);
			}
			String date = "";
			if (rs1 == null) {
				throw new UnexpectedLiquibaseException(
						"Unexpected null result set when getting last executed changeset date");
			}
			while (rs1.next()) {
				date =  rs1.getString("LAST_DATEEXECUTED");
			}
			rs1.close();
			if (ps1 != null) {
				ps1.close();
			}

			//Query to get composite key details of last executed change set
			String query2 = "SELECT id, author, filename FROM " + databaseChangelogTableName
					+ " WHERE dateexecuted = '" + date + "' ALLOW FILTERING";
			ResultSet rs2 = statement1.executeQuery(query2);
			String id = "", author = "", filename = "";
			while (rs2.next()) {
				id = rs2.getString("id");
				author = rs2.getString("author");
				filename = rs2.getString("filename");
			}
			rs2.close();
			statement1.close();

			//Query to update tag 
			String updateQuery = "UPDATE " 
					+ databaseChangelogTableName
					+ " SET TAG = " + tagEscaped
					+ " WHERE id = '" + id + "' AND author = '" + author + "' AND filename = '" + filename + "'";

			return new Sql[]{
				new UnparsedSql(updateQuery)
			};

		} catch (SQLException | DatabaseException e) {
			return super.generateSql(statement, database, sqlGeneratorChain);
		} finally {
			database.setObjectQuotingStrategy(currentStrategy);
		}
	}

	/**
	 * Gets the timestamp of the last executed changeset without using {@code MAX} aggregate function.
	 *
	 * @implNote Since aggregate functions like MAX are not supported by AWS Keyspaces (see
	 * <a href="https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html#cassandra-functions">
	 * Cassandra functions in AWS Keyspaces</a>), this method tries to get all the values of the column DATEEXECUTED in
	 * the table of the executed changesets to calculate and return the maximal value of this column.
	 *
	 * @param database 			 The database where the query is executed.
	 * @param changelogTableName The name of the changelog table.
	 * @return The timestamp of the last executed changeset.
	 * @throws SQLException in case something goes wrong during the query execution.
	 * @throws DatabaseException in case something goes wrong during the query execution.
	 */
	private Timestamp selectLastExecutedChangesetTimestamp(final Database database, final String changelogTableName)
			throws SQLException, DatabaseException {
		Statement statement = ((CassandraDatabase) database).getStatement();
		ResultSet rs = statement.executeQuery("SELECT DATEEXECUTED FROM " + changelogTableName);
		Timestamp maxValue = null;
		while (rs.next()) {
			Timestamp result = rs.getTimestamp("DATEEXECUTED");
			if (maxValue == null || maxValue.compareTo(result) < 0) {
				maxValue = result;
			}
		}
		rs.close();
		statement.close();
		return maxValue;
	}

}
