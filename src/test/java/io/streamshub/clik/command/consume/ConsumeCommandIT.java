package io.streamshub.clik.command.consume;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
class ConsumeCommandIT extends ConsumeCommandTest {
    // Runs all tests from ConsumeCommandTest against native executable
}
