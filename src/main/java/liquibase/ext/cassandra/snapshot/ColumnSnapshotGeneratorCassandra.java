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
import java.util.stream.Collectors;

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
            String query = String.format("SELECT KEYSPACE_NAME, COLUMN_NAME, TYPE, KIND FROM system_schema.columns WHERE KEYSPACE_NAME = '%s' AND table_name='%s';"
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
        //we can't add column name as query parameter here as AWS keyspaces don't support such where statement
        String query = String.format("SELECT KEYSPACE_NAME, COLUMN_NAME, TYPE, KIND FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name='%s';"
                , database.getDefaultCatalogName(), relation);

        List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database).queryForList(new RawSqlStatement(query));
        returnList = returnList.stream()
                .filter(stringMap -> ((String)stringMap.get("COLUMN_NAME")).equalsIgnoreCase(example.getName()))
                .collect(Collectors.toList());
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
        // we don't really need KEYSPACE_NAME param in query to build Column obj, but Astra Cassandra implementation
        // (and maybe some others) fails if it's missing
        Column column = new Column();
        column.setName(rawColumnName);
        column.setRelation(table);
        // Cassandra doesn't actually store somewhere separately if column could nullable or not,
        // but it doesn't allow primary keys to be missing, so gonna use this field as nullable indicator
        column.setNullable(!"partition_key".equalsIgnoreCase(rawColumnKind));
        //TODO extend datatype parsing when needed to include DataTypeId
        column.setType(new DataType(rawColumnType));
        return column;
    }

}
