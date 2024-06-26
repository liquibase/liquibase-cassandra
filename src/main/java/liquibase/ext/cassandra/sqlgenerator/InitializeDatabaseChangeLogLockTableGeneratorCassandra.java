package liquibase.ext.cassandra.sqlgenerator;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.ext.cassandra.lockservice.LockServiceCassandra;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.RawSqlStatement;

import static liquibase.ext.cassandra.database.CassandraDatabase.isAwsKeyspacesCompatibilityModeEnabled;
import static liquibase.ext.cassandra.sqlgenerator.CreateDatabaseChangeLogLockTableGeneratorCassandra.buildCreateTableStatement;

public class InitializeDatabaseChangeLogLockTableGeneratorCassandra extends InitializeDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(InitializeDatabaseChangeLogLockTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof CassandraDatabase;
    }

    @Override
    public Sql[] generateSql(InitializeDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        SqlStatement[] statements;

        String databaseChangelogLockTableName = database.escapeTableName(
                database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(),
                database.getDatabaseChangeLogLockTableName().toUpperCase());

        InsertStatement insertStatement = new InsertStatement(database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName().toUpperCase())
                .addColumnValue("ID", 1)
                .addColumnValue("LOCKED", Boolean.FALSE);

        if (isAwsKeyspacesCompatibilityModeEnabled()) {
            // Since AWS Keyspaces does not support TRUNCATE TABLE statements
            // (https://docs.aws.amazon.com/keyspaces/latest/devguide/cassandra-apis.html#cassandra-api-support), drop
            // then re-create the changelog lock table when the AWS Keyspaces compatibility mode is enabled.
            Scope.getCurrentScope().getLog(InitializeDatabaseChangeLogLockTableGeneratorCassandra.class)
                    .fine("AWS Keyspaces compatibility mode enabled: using alternative queries to truncate changelog lock table");
            RawSqlStatement dropStatement = new RawSqlStatement("DROP TABLE " + databaseChangelogLockTableName);
            RawSqlStatement createStatement = buildCreateTableStatement(databaseChangelogLockTableName);

            statements = new SqlStatement[]{dropStatement, createStatement, insertStatement};
        } else {
            RawSqlStatement deleteStatement = new RawSqlStatement("TRUNCATE TABLE " + databaseChangelogLockTableName);

            statements = new SqlStatement[]{deleteStatement, insertStatement};
        }

        return SqlGeneratorFactory.getInstance().generateSql(statements, database);

    }

}
