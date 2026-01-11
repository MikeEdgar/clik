package io.streamshub.clik.command.cluster;

import org.junit.jupiter.api.Tag;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
@Tag("integration-test")
class ClusterCommandIT extends ClusterCommandTest {
    // Inherits all test methods from ClusterCommandTest
    // This class runs the same tests against the built artifact (JAR or native executable)
}
