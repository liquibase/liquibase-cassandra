package liquibase.ext.cassandra.snapshot;

import liquibase.database.Database;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.structure.DatabaseObject;

public class ForeignKeySnapshotGeneratorCassandra extends ForeignKeySnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof CassandraDatabase) {
            int priority = super.getPriority(objectType, database);
            return priority == 0 ? priority : priority + PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) {
        // Cassandra does not support foreign keys, so we do nothing
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) {
        return null;
    }
}