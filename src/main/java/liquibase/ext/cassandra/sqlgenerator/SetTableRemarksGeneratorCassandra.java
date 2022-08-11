package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.MySQLDatabase;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetTableRemarksGenerator;
import liquibase.statement.core.SetTableRemarksStatement;
import liquibase.structure.DatabaseObject;
import liquibase.util.StringUtil;

public class SetTableRemarksGeneratorCassandra extends SetTableRemarksGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(SetTableRemarksStatement statement, Database database) {
        return database instanceof CassandraDatabase;
    }

    public Sql[] generateSql(SetTableRemarksStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String remarksEscaped = database.escapeStringForDatabase(StringUtil.trimToEmpty(statement.getRemarks()));
        String sql = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " WITH comment = '" + remarksEscaped + "'";
        return new Sql[]{new UnparsedSql(sql, this.getAffectedTable(statement))};
    }
}
