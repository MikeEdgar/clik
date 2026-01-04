package io.streamshub.clik.command.context;

import org.junit.jupiter.api.Tag;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
@Tag("integration-test")
class ContextCommandIT extends ContextCommandTest {
    // Inherits all test methods from ContextCommandTest
    // This class runs the same tests against the built artifact (JAR or native executable)
}
