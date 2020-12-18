package liquibase.ext.cassandra.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.snapshot.*;
import liquibase.snapshot.jvm.TableSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.util.StringUtil;

import java.util.List;
import java.util.Map;

public class TableSnapshotGeneratorCassandra extends TableSnapshotGenerator {
    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException {
        if (!snapshot.getSnapshotControl().shouldInclude(Table.class)) {
            return;
        }
        if (foundObject instanceof Schema) {

            Database database = snapshot.getDatabase();
            Schema schema = (Schema) foundObject;

            //TODO replace * when we know which fields we need
            String query = String.format("SELECT * FROM system_schema.tables WHERE keyspace_name = '%s';",
                    database.getDefaultCatalogName());
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",
                    database);
            List<Map<String, ?>> returnList = executor.queryForList(new RawSqlStatement(query));

            for (Map<String, ?> tablePropertiesMap : returnList) {
                schema.addDatabaseObject(readTable(tablePropertiesMap, database));
            }

        }


    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException {
        Database database = snapshot.getDatabase();

        //TODO replace * when we know which fields we need
        String query =
                String.format("SELECT * FROM system_schema.tables WHERE keyspace_name = '%s' AND TABLE_NAME = '%s'",
                        example.getName().toLowerCase(), database.getDefaultCatalogName());
        List<Map<String, ?>> returnList = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database).queryForList(new RawSqlStatement(query));
        if (returnList.size() != 1) {
            LogFactory.getLogger().info(String.format("expecting exactly 1 table with name {}, got {}",
                    example.getName(), returnList.size()));
            return null;
        } else {
            return readTable(returnList.get(0), database);

        }
    }

    protected Table readTable(Map<String, ?> tableMap, Database database) {

        String rawTableName = StringUtil.trimToNull((String) tableMap.get("TABLE_NAME"));
        String rawKeyspaceName = StringUtil.trimToNull((String) tableMap.get("KEYSPACE_NAME"));
        String comment = StringUtil.trimToNull((String) tableMap.get("COMMENT"));

        Table table = new Table();
        table.setName(cleanNameFromDatabase(rawTableName, database));
        table.setRemarks(comment);

        CatalogAndSchema schemaFromJdbcInfo = ((AbstractJdbcDatabase) database).getSchemaFromJdbcInfo(rawKeyspaceName
                , null);
        table.setSchema(new Schema(schemaFromJdbcInfo.getCatalogName(), schemaFromJdbcInfo.getSchemaName()));

        return table;
    }

}
