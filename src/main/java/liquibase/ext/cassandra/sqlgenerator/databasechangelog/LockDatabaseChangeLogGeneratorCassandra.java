package liquibase.ext.cassandra.sqlgenerator.databasechangelog;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.InsertOrUpdateStatement;
import liquibase.statement.core.LockDatabaseChangeLogStatement;

public class LockDatabaseChangeLogGeneratorCassandra extends LockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String liquibaseSchema = database.getLiquibaseSchemaName();
        String liquibaseCatalog = database.getLiquibaseCatalogName();

        InsertOrUpdateStatement updateStatement = new InsertOrUpdateStatement(liquibaseCatalog, liquibaseSchema, database.getDatabaseChangeLogLockTableName(),"id");

        updateStatement.addColumnValue("LOCKED", true);
        updateStatement.addColumnValue("LOCKEDBY", hostname + " (" + hostaddress + ")");
        updateStatement.addColumnValue("LOCKGRANTED", System.currentTimeMillis());
        updateStatement.addColumnValue("ID", 1);

        return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);

    }

}
