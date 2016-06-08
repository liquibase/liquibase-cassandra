package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.statement.core.CreateTableStatement;

import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public class CassandraCreateTableGenerator extends CreateTableGenerator {

    final DataTypeFactory instance = DataTypeFactory.getInstance();

    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return database.getDatabaseProductName().equals(CassandraDatabase.PRODUCT_NAME);
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        final StringBuffer buffer = new StringBuffer();
        buffer.append("CREATE TABLE ").append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())).append(" ");
        buffer.append("(");
        String columnsAsString = statement.getColumnTypes().entrySet().stream().map(columnEntry -> {
            LiquibaseDataType value = columnEntry.getValue();
            String cassandraColumn = String.format("%s %s",columnEntry.getKey(),value.getName());
            return cassandraColumn;
        }).collect(joining(","));
        buffer.append(columnsAsString);
        buffer.append(",");
        buffer.append(String.format(", %s",getPrimaryKey(statement)));
        buffer.append(")");
        return new Sql[]{new UnparsedSql(buffer.toString())};
    }

    private String getPrimaryKey(CreateTableStatement statement) {
        return String.format("PRIMARY KEY(%s)",statement.getPrimaryKeyConstraint().getColumns().stream().collect(Collectors.joining(",")));
    }

}
