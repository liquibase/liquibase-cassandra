package liquibase.ext.cassandra.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.sqlgenerator.core.GetNextChangeSetSequenceValueGenerator;
import liquibase.statement.core.GetNextChangeSetSequenceValueStatement;

public class GetNextChangeSetSequenceValueGeneratorCassandra extends GetNextChangeSetSequenceValueGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(GetNextChangeSetSequenceValueStatement statement, Database database) {
        return super.supports(statement, database) && database instanceof CassandraDatabase;
    }



}
