package liquibase.harness.snapshot.cassandra

import liquibase.harness.snapshot.SnapshotTest
import liquibase.snapshot.DatabaseSnapshot
import liquibase.structure.core.Table

[
        [
                setup : "create table test_table (test_col int PRIMARY KEY, col2 varchar)",
                verify: {
                    DatabaseSnapshot snapshot ->
                        snapshot.get(new Table(name: "test_table")).with {
                            assert name.equalsIgnoreCase("test_table")
                            assert columns*.name.containsAll(["test_col", "col2"])
                        }
                }
        ],
] as SnapshotTest.TestConfig[]