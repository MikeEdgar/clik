package io.streamshub.clik.command.produce;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
class ProduceCommandIT extends ProduceCommandTest {
    // Runs all tests from ProduceCommandTest against native executable
}
