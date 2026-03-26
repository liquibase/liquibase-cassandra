package liquibase.ext.cassandra.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ViewSnapshotGenerator;
import liquibase.structure.DatabaseObject;


public class ViewSnapshotGeneratorCassandra extends ViewSnapshotGenerator {

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
        return new Class[]{ViewSnapshotGenerator.class};
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException {
        // Cassandra does not support views (materialized views are not standard SQL views).
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        return null;
    }
}
