package liquibase.ext.cassandra.sqlgenerator;

import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.RawSqlStatement;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateDatabaseChangeLogLockTableGeneratorCassandra extends CreateDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof CassandraDatabase;
    }

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	
    	RawSqlStatement createTableStatement = new RawSqlStatement("CREATE TABLE IF NOT EXISTS " + CassandraUtil.getKeyspace(database) + ".DATABASECHANGELOGLOCK (ID INT, LOCKED BOOLEAN, LOCKGRANTED timestamp, LOCKEDBY TEXT, PRIMARY KEY (ID))");
        return SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database);
        
    }
    

}
