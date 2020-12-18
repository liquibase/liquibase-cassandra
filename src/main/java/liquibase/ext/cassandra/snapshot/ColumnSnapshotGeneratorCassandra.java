package liquibase.ext.cassandra.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Relation;
import liquibase.util.StringUtil;

import java.util.List;
import java.util.Map;

public class ColumnSnapshotGeneratorCassandra extends ColumnSnapshotGenerator {
    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException {
        if (!snapshot.getSnapshotControl().shouldInclude(Column.class)) {
            return;
        }
        if (foundObject instanceof Relation) {

            Database database = snapshot.getDatabase();
            Relation relation = (Relation) foundObject;
            //TODO replace * when we know which fields we need
            String query = String.format("SELECT * FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name='%s';"
                    , database.getDefaultCatalogName(), relation.getName());
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",
                    database);
            List<Map<String, ?>> returnList = executor.queryForList(new RawSqlStatement(query));

            for (Map<String, ?> columnPropertiesMap : returnList) {
                relation.getColumns().add(readColumn(columnPropertiesMap, relation, database));
            }

        }


    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Database database = snapshot.getDatabase();
        Relation relation = ((Column) example).getRelation();
        //TODO replace * when we know which fields we need
        String query = String.format("SELECT * FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name='%s' AND column_name='%s';"
                , database.getDefaultCatalogName(), relation, example.getName());

        List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database).queryForList(new RawSqlStatement(query));
        if (returnList.size() != 1) {
            LogFactory.getLogger().info(String.format("expecting exactly 1 column with name {}, got {}",
                    example.getName(), returnList.size()));
            return null;
        } else {
            return readColumn(returnList.get(0), relation, database);

        }
    }

    protected Column readColumn(Map<String, ?> tableMap, Relation table, Database database) {

        String rawColumnName = StringUtil.trimToNull((String) tableMap.get("COLUMN_NAME"));


        Column column = new Column();
        column.setName(rawColumnName);
        column.setRelation(table);
        return column;
    }

}
