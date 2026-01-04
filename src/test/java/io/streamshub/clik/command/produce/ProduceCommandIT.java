package io.streamshub.clik.command.produce;

import org.junit.jupiter.api.Tag;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
@Tag("integration-test")
class ProduceCommandIT extends ProduceCommandTest {
    // Runs all tests from ProduceCommandTest against native executable
}
