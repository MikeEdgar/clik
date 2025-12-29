package io.streamshub.clik.command.topic;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;
import io.streamshub.clik.test.ClikMainTestBase;

@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
class TopicCommandIT extends TopicCommandTest {
    // Inherits all test methods from TopicCommandTest
    // This class runs the same tests against the built artifact (JAR or native executable)
}
