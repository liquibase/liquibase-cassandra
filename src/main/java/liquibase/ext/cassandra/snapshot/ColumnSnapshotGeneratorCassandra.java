package liquibase.ext.cassandra.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Relation;
import liquibase.util.StringUtil;

import java.util.List;
import java.util.Map;

public class ColumnSnapshotGeneratorCassandra extends ColumnSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof CassandraDatabase) {
            return super.getPriority(objectType, database);
        }
        return PRIORITY_NONE;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException {
        if (!snapshot.getSnapshotControl().shouldInclude(Column.class)) {
            return;
        }
        if (foundObject instanceof Relation) {
            Database database = snapshot.getDatabase();
            Relation relation = (Relation) foundObject;
            String query = String.format("SELECT COLUMN_NAME, TYPE, KIND FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name='%s';"
                    , database.getDefaultCatalogName(), relation.getName());
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",
                    database);
            List<Map<String, ?>> returnList = executor.queryForList(new RawSqlStatement(query));

            for (Map<String, ?> columnPropertiesMap : returnList) {
                relation.getColumns().add(readColumn(columnPropertiesMap, relation));
            }
        }
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Database database = snapshot.getDatabase();
        Relation relation = ((Column) example).getRelation();
        String query = String.format("SELECT COLUMN_NAME, TYPE, KIND FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name='%s' AND column_name='%s';"
                , database.getDefaultCatalogName(), relation, example.getName());

        List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database).queryForList(new RawSqlStatement(query));
        if (returnList.size() != 1) {
            Scope.getCurrentScope().getLog(ColumnSnapshotGeneratorCassandra.class).warning(String.format(
                    "expecting exactly 1 column with name %s, got %s", example.getName(), returnList.size()));
            return null;
        } else {
            return readColumn(returnList.get(0), relation);
        }
    }

    protected Column readColumn(Map<String, ?> tableMap, Relation table) {

        String rawColumnName = StringUtil.trimToNull((String) tableMap.get("COLUMN_NAME"));
        String rawColumnType = StringUtil.trimToNull((String) tableMap.get("TYPE"));
        String rawColumnKind = StringUtil.trimToNull((String) tableMap.get("KIND"));
        Column column = new Column();
        column.setName(rawColumnName);
        column.setRelation(table);
        // Cassandra doesn't actually store somewhere separately if column could nullable or not,
        // but it doesn't allow primary keys to be missing, so gonna use this field as nullable indicator
        column.setNullable("partition_key".equalsIgnoreCase(rawColumnKind));
        //TODO extend datatype parsing when needed to include DataTypeId
        column.setType(new DataType(rawColumnType));
        return column;
    }

}
