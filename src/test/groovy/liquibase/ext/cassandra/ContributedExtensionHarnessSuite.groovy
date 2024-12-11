package liquibase.ext.cassandra

import liquibase.harness.BaseHarnessSuite
import liquibase.harness.change.ChangeObjectTests
import org.junit.platform.suite.api.SelectClasses

@SelectClasses(ChangeObjectTests.class)
class ContributedExtensionHarnessSuite extends BaseHarnessSuite {

}