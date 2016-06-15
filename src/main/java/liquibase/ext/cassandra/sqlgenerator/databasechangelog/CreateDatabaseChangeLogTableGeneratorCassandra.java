package liquibase.ext.cassandra.sqlgenerator.databasechangelog;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.CreateTableStatement;

public class CreateDatabaseChangeLogTableGeneratorCassandra extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String charTypeName = getCharTypeName(database);
        String dateTimeTypeString = getDateTimeTypeString(database);
        CreateTableStatement createTableStatement = new CreateTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
                .setTablespace(database.getLiquibaseTablespaceName())
                .addPrimaryKeyColumn("ID", DataTypeFactory.getInstance().fromDescription(charTypeName, database), null, null, null)
                .addPrimaryKeyColumn("AUTHOR", DataTypeFactory.getInstance().fromDescription(charTypeName, database), null, null, null)
                .addColumn("FILENAME", DataTypeFactory.getInstance().fromDescription(charTypeName, database), null, null, new NotNullConstraint())
                .addColumn("DATEEXECUTED", DataTypeFactory.getInstance().fromDescription(dateTimeTypeString, database), null, null, new NotNullConstraint())
                .addColumn("ORDEREXECUTED", DataTypeFactory.getInstance().fromDescription("int", database), null, null, new NotNullConstraint())
                .addColumn("EXECTYPE", DataTypeFactory.getInstance().fromDescription(charTypeName, database), null, null, new NotNullConstraint())
                .addColumn("MD5SUM", DataTypeFactory.getInstance().fromDescription(charTypeName, database))
                .addColumn("DESCRIPTION", DataTypeFactory.getInstance().fromDescription(charTypeName, database))
                .addColumn("COMMENTS", DataTypeFactory.getInstance().fromDescription(charTypeName, database))
                .addColumn("TAG", DataTypeFactory.getInstance().fromDescription(charTypeName, database))
                .addColumn("LIQUIBASE", DataTypeFactory.getInstance().fromDescription(charTypeName, database))
                .addColumn("CONTEXTS", DataTypeFactory.getInstance().fromDescription(charTypeName, database))
                .addColumn("LABELS", DataTypeFactory.getInstance().fromDescription(charTypeName, database));

        return SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database);
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
}
