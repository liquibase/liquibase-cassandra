package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.RawSqlStatement;

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

        RawSqlStatement deleteStatement = new RawSqlStatement("TRUNCATE TABLE " + database.escapeTableName(
                database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(),
                database.getDatabaseChangeLogLockTableName().toUpperCase()));

        return SqlGeneratorFactory.getInstance().generateSql(deleteStatement, database);

    }


}
