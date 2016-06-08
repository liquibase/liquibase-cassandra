package liquibase.ext.cassandra.database;

public class CassandraQueries {
    public static final String SELECT_DATABASE_CHANGE_LOCK = "select ID from DATABASECHANGELOGLOCK";
    public static final String CREATE_DATABASE_CHANGE_LOCK = "CREATE TABLE DATABASECHANGELOGLOCK (ID INT PRIMARY KEY, LOCKED BOOLEAN, LOCKGRANTED timestamp, LOCKEDBY TEXT)";
}
