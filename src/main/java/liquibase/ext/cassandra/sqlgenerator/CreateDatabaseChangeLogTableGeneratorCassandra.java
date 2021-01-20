package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.RawSqlStatement;

public class CreateDatabaseChangeLogTableGeneratorCassandra extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof CassandraDatabase;
    }

    @Override
    protected String getCharTypeName(Database database) {
        return "TEXT";
    }

    @Override
    protected String getDateTimeTypeString(Database database) {
        return "TIMESTAMP";
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {


        RawSqlStatement createTableStatement = new RawSqlStatement("CREATE TABLE IF NOT EXISTS " +
                database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), "databasechangelog") +
                "( ID TEXT, AUTHOR TEXT, FILENAME TEXT, DATEEXECUTED timestamp, ORDEREXECUTED INT, EXECTYPE TEXT, MD5SUM TEXT, DESCRIPTION TEXT, COMMENTS TEXT, TAG TEXT, LIQUIBASE TEXT, CONTEXTS TEXT, LABELS TEXT, DEPLOYMENT_ID TEXT,PRIMARY KEY (ID, AUTHOR, FILENAME))");

        return SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database);

    }

}
