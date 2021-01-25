package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.statement.core.RawSqlStatement;

public class LockDatabaseChangeLogGeneratorCassandra extends LockDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        RawSqlStatement updateStatement = new RawSqlStatement("UPDATE " +
                database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), "databasechangeloglock") +
                " SET LOCKED = TRUE, LOCKEDBY = '" + hostname + " (" + hostaddress + ")" + "', LOCKGRANTED = " + System.currentTimeMillis() + " WHERE ID = 1");
        return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
    }

}
