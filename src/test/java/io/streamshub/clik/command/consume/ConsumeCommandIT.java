package io.streamshub.clik.command.consume;

import org.junit.jupiter.api.Tag;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
@Tag("integration-test")
class ConsumeCommandIT extends ConsumeCommandTest {
    // Runs all tests from ConsumeCommandTest against native executable
}
