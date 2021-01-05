package liquibase.ext.cassandra.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.cassandra.database.CassandraDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.jvm.IndexSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.StringUtil;


import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IndexSnapshotGeneratorCassandra extends IndexSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        int priority = super.getPriority(objectType, database);
        if (database instanceof CassandraDatabase) {
            priority += PRIORITY_DATABASE;
        }
        return priority;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(Index.class)) {
            return;
        }
        if (foundObject instanceof Relation) {

            Relation relation = (Relation) foundObject;
            Database database = snapshot.getDatabase();

            String query = String.format("SELECT INDEX_NAME, OPTIONS FROM system_schema.indexes WHERE keyspace_name = '%s' AND TABLE_NAME='%s';",
                    database.getDefaultCatalogName(), relation.getName());
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",
                    database);
            List<Map<String, ?>> returnList = executor.queryForList(new RawSqlStatement(query));
            for (Map<String, ?> tablePropertiesMap : returnList) {
                relation.getIndexes().add(readIndex(tablePropertiesMap, relation));
            }
        }
    }

    private Index readIndex(Map<String, ?> tableMap, Relation relation) {
        String indexName = StringUtil.trimToNull((String) tableMap.get("INDEX_NAME"));
        String options = StringUtil.trimToNull((String) tableMap.get("OPTIONS"));

        Index index = new Index();
        index.setName(indexName);
        index.setRelation(relation);
        index.setColumns(parseColumns(options));
        return index;
    }

    private List<Column> parseColumns(String options) {
//         options is kinda json but not RFC specification compatible, looks like this
//        {
//            'analyzer_class':org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer,
//            'case_sensitive':false,
//            'class_name':org.apache.cassandra.index.sasi.SASIIndex,
//            'mode':CONTAINS,
//            'target':first_name
//        }
//      standard parsers don't work, so it's easier to parse string manually

        int startIndex = options.indexOf("target");
        String[] keyValues = options.substring(startIndex, options.indexOf("}", startIndex)).split(":");

        if (keyValues.length == 2) {
            return Collections.singletonList(new Column(keyValues[1]));
        }
        return Collections.emptyList();
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        Relation relation = ((Index) example).getRelation();
        Database database = snapshot.getDatabase();

        String query = String.format("SELECT INDEX_NAME, OPTIONS FROM system_schema.indexes WHERE keyspace_name = '%s' AND TABLE_NAME='%s' AND INDEX_NAME= '%s';",
                database.getDefaultCatalogName(), relation.getName(), example.getName());
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc",
                database);
        List<Map<String, ?>> returnList = executor.queryForList(new RawSqlStatement(query));
        if (returnList.size() != 1) {
            Scope.getCurrentScope().getLog(TableSnapshotGeneratorCassandra.class).warning(String.format(
                    "expecting exactly 1 index with name %s, got %s", example.getName(), returnList.size()));
            return null;
        } else {
            return readIndex(returnList.get(0), relation);
        }
    }


}
