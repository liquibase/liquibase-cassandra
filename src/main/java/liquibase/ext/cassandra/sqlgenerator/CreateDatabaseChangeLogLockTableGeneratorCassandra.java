package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.RawSqlStatement;

public class CreateDatabaseChangeLogLockTableGeneratorCassandra extends CreateDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof CassandraDatabase;
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        RawSqlStatement createTableStatement = buildCreateTableStatement(
                database.escapeTableName(
                        database.getLiquibaseCatalogName(),
                        database.getLiquibaseSchemaName(),
                        database.getDatabaseChangeLogLockTableName())
        );

        return SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database);

    }

    protected static RawSqlStatement buildCreateTableStatement(String tableName) {
        return new RawSqlStatement("CREATE TABLE IF NOT EXISTS " + tableName
                + " (ID INT, LOCKED BOOLEAN, LOCKGRANTED timestamp, LOCKEDBY TEXT, PRIMARY KEY (ID))");
    }

}
