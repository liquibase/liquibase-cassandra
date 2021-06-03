package liquibase.harness.snapshot.cassandra

import liquibase.harness.snapshot.SnapshotTest
import liquibase.snapshot.DatabaseSnapshot
import liquibase.structure.core.Column
import liquibase.structure.core.Table

[
        [
                setup : "create table test_table (test_col int PRIMARY KEY)",
                verify: {
                    DatabaseSnapshot snapshot ->
                        snapshot.get(new Column(Table.class, null, null, "test_table", "test_col")).with {
                            assert type.typeName.toLowerCase().startsWith("int")
                        }

                }
        ],
        [
                setup : "create table test_table (test_col varchar PRIMARY KEY)",
                verify: {
                    DatabaseSnapshot snapshot ->
                        snapshot.get(new Column(Table.class, null, null, "test_table", "test_col")).with {
                            assert type.typeName.toLowerCase() == "text"
                        }
                }
        ],
        [
                setup : "create table test_table (test_col int PRIMARY KEY)",
                verify: { DatabaseSnapshot snapshot ->
                    snapshot.get(new Column(Table.class, null, null, "test_table", "test_col")).with {
                        assert !nullable
                    }
                }
        ],
] as SnapshotTest.TestConfig[]

