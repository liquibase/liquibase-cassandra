package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;

public class UnlockDatabaseChangeLogGeneratorCassandra extends UnlockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    public Sql[] generateSql(UnlockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String liquibaseSchema = database.getLiquibaseSchemaName();

        UpdateStatement releaseStatement = new UpdateStatement(database.getLiquibaseCatalogName(), liquibaseSchema, database.getDatabaseChangeLogLockTableName());
        releaseStatement.addNewColumnValue("LOCKED", false);
        releaseStatement.addNewColumnValue("LOCKEDBY", null);
        releaseStatement.setWhereClause(database.escapeColumnName(database.getLiquibaseCatalogName(), liquibaseSchema, database.getDatabaseChangeLogTableName(), "ID")+" = 1");

        return SqlGeneratorFactory.getInstance().generateSql(releaseStatement, database);
    }

}
