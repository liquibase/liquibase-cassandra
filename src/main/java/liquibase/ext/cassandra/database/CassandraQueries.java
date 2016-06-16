package liquibase.ext.cassandra.database;

public class CassandraQueries {
    public static final String SELECT_ID_LOCKED_FROM_LIQUIBASE_DATABASECHANGELOGLOCK = "select ID,LOCKED from liquibase.DATABASECHANGELOGLOCK";
    public static final String SELECT_ID_FROM_DATABASECHANGELOG = "select ID from DATABASECHANGELOG";
}
