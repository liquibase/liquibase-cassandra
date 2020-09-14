package liquibase.ext.cassandra.lockservice;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ClearDatabaseChangeLogTableGenerator;
import liquibase.statement.core.ClearDatabaseChangeLogTableStatement;

public class ClearDatabaseChangeLogTableGeneratorCassandra extends ClearDatabaseChangeLogTableGenerator {
	
	
	@Override
	public Sql[] generateSql(ClearDatabaseChangeLogTableStatement statement, Database database,
			SqlGeneratorChain sqlGeneratorChain) {
		// TODO Auto-generated method stub
		return super.generateSql(statement, database, sqlGeneratorChain);
	}
	

}
