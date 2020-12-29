package liquibase.ext.cassandra.sqlgenerator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.RawSqlStatement;

public class InitializeDatabaseChangeLogLockTableGeneratorCassandra extends InitializeDatabaseChangeLogLockTableGenerator{
	
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
	
    @Override
    public Sql[] generateSql(InitializeDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	
    	
    	RawSqlStatement deleteStatement = new RawSqlStatement("TRUNCATE " + database.escapeTableName(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), "databasechangeloglock"));

    	List<Sql> sql = new ArrayList<>();

        sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(deleteStatement, database)));
        
        return sql.toArray(new Sql[sql.size()]);
    }
	

}
