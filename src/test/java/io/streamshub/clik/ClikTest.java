package io.streamshub.clik;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusMainTest
@TestProfile(ClikTest.TestConfig.class)
class ClikTest {

    private static String generatedVersion = UUID.randomUUID().toString();

    public static class TestConfig implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.application.version", generatedVersion);
        }
    }

    @Test
    @Launch({"--version"})
    void testCreateContextWithBootstrapServers(LaunchResult result) {
        assertEquals(0, result.exitCode());
        assertEquals(generatedVersion, result.getOutput().trim());
    }
}
