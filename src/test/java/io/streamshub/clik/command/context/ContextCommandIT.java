package io.streamshub.clik.command.context;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
class ContextCommandIT extends ContextCommandTest {
    // Inherits all test methods from ContextCommandTest
    // This class runs the same tests against the built artifact (JAR or native executable)
}
