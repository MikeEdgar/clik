package io.streamshub.clik.command.group;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikTestBase.Profile.class)
class GroupCommandIT extends GroupCommandTest {
    // Inherits all test methods from GroupCommandTest
    // This class runs the same tests against the built artifact (JAR or native executable)
}
