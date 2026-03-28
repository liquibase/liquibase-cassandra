package liquibase.ext.cassandra.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
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
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{ForeignKeySnapshotGenerator.class};
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException {
        // Cassandra does not support foreign key constraints.
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        return null;
    }
}
