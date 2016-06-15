package liquibase.ext.cassandra.sqlgenerator.databasechangelog;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SelectFromDatabaseChangeLogGenerator;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;

public class SelectFromDatabaseChangeLogGeneratorCassandra extends SelectFromDatabaseChangeLogGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(SelectFromDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Sql[] sqlGenerate = super.generateSql(statement,database,sqlGeneratorChain);
        return adaptToCassandra(sqlGenerate);
    }

    private Sql[] adaptToCassandra(Sql[] sqlGenerate) {
        String sql = removeInvalidFilters(sqlGenerate[0].toSql());
        return new Sql []{new UnparsedSql(sql)};
    }

    private String removeInvalidFilters(String sql) {
        return sql.split("WHERE")[0];
    }

}
