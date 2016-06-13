
package liquibase.ext.cassandra.sqlgenerator.cql;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.DeleteStatement;

public class DeleteTableGenerator extends liquibase.sqlgenerator.core.DeleteGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(DeleteStatement statement, Database database) {
        return database.getDatabaseProductName().equals(CassandraDatabase.PRODUCT_NAME);
    }

    @Override
    public Sql[] generateSql(DeleteStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String truncateTableCql = String.format("TRUNCATE %s", database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));
        return new Sql[]{new UnparsedSql(truncateTableCql)};
    }
}
