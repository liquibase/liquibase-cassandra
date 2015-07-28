package liquibase.ext.cassandra.database;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class CassandraDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("cassandra", new CassandraDatabase().getShortName());
    }

}
