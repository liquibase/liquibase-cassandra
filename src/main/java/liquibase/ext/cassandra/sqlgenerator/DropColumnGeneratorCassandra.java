package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropColumnGenerator;
import liquibase.statement.core.DropColumnStatement;

import java.util.ArrayList;
import java.util.List;

public class DropColumnGeneratorCassandra extends DropColumnGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(DropColumnStatement statement, Database database) {
        return database instanceof CassandraDatabase;
    }

    @Override
    public Sql[] generateSql(DropColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        if (statement.isMultiple()) {
            return generateMultipleColumnSql(statement.getColumns(), database);
        } else {
            return generateSingleColumnSql(statement, database);
        }
    }

    private Sql[] generateMultipleColumnSql(List<DropColumnStatement> columns, Database database) {
        List<Sql> result = new ArrayList<>();
        for (DropColumnStatement column : columns) {
            result.add(generateSingleColumnSql(column, database)[0]);
        }
        return result.toArray(new Sql[result.size()]);
    }

    private Sql[] generateSingleColumnSql(DropColumnStatement statement, Database database) {
        return new Sql[]{new UnparsedSql("ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " DROP " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()), getAffectedColumn(statement))};
    }
}
