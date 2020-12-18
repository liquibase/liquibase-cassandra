import liquibase.sdk.test.ChangeObjectTests
import liquibase.sdk.test.config.TestConfig

import java.util.stream.Collectors

class LiquibaseSdkSuite extends ChangeObjectTests {
    def setup() {
        TestConfig.instance.databasesUnderTest = TestConfig.instance.databasesUnderTest.stream()
                .map({ underTest -> underTest.database.outputDefaultCatalog = true })
        .collect(Collectors.toList())
    }
}