package liquibase.ext.cassandra.sqlgenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.change.core.AddPrimaryKeyChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.InsertStatement;

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
    
    public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    	
    	
    	/*
    	 CREATE TABLE DATABASECHANGELOG ( 
			ID TEXT, 
			AUTHOR VARCHAR, 
			FILENAME VARCHAR, 
			DATEEXECUTED timestamp, 
			ORDEREXECUTED INT, 
			EXECTYPE VARCHAR, 
			MD5SUM VARCHAR, 
			DESCRIPTION VARCHAR, 
			COMMENTS VARCHAR, 
			TAG VARCHAR, 
			LIQUIBASE VARCHAR, 
			CONTEXTS VARCHAR, 
			LABELS VARCHAR, 
			DEPLOYMENT_ID VARCHAR,
			PRIMARY KEY (ID)
			);
    	 */
        CreateTableStatement createTableStatement = new CreateTableStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName())
                .setTablespace(database.getLiquibaseTablespaceName())
                

//                .addColumn("ID", DataTypeFactory.getInstance().fromDescription("INT", database)) 
                .addColumn("FOO", DataTypeFactory.getInstance().fromDescription("INT", database)) 
		    	.addColumn("AUTHOR", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("FILENAME", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		        .addColumn("DATEEXECUTED", DataTypeFactory.getInstance().fromDescription("TIMESTAMP", database))
		        .addColumn("ORDEREXECUTED", DataTypeFactory.getInstance().fromDescription("INT", database))
		    	.addColumn("EXECTYPE", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("MD5SUM", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("DESCRIPTION", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("COMMENTS", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("TAG", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("LIQUIBASE", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("CONTEXTS", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("LABELS", DataTypeFactory.getInstance().fromDescription("TEXT", database))
		    	.addColumn("DEPLOYMENT_ID", DataTypeFactory.getInstance().fromDescription("TEXT", database));

        		//PRIMARY KEY (ID)
        
        

        // no support for AND in update
        InsertStatement insertStatement = new InsertStatement(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName());

        List<Sql> sql = new ArrayList<Sql>();

        sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(createTableStatement, database)));
        sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(insertStatement, database)));

        return sql.toArray(new Sql[sql.size()]);
    }    
    
}
