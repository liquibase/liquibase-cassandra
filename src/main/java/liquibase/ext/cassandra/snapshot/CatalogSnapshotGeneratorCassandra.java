package liquibase.ext.cassandra.snapshot;

import liquibase.exception.DatabaseException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.jvm.CatalogSnapshotGenerator;
import liquibase.structure.DatabaseObject;

public class CatalogSnapshotGeneratorCassandra extends CatalogSnapshotGenerator {
    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        return super.snapshotObject(example, snapshot);
    }

}
