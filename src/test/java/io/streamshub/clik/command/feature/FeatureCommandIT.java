package io.streamshub.clik.command.feature;

import org.junit.jupiter.api.Tag;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
@Tag("integration-test")
class FeatureCommandIT extends FeatureCommandTest {
    // Inherits all test methods from FeatureCommandTest
    // This class runs the same tests against the built artifact (JAR or native executable)
}
