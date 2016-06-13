package liquibase.ext.cassandra.database;

public class CassandraQueries {
    public static final String SELECT_DATABASE_CHANGE_LOCK = "select ID,LOCKED from liquibase.DATABASECHANGELOGLOCK";
}
