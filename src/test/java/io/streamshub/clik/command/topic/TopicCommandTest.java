package io.streamshub.clik.command.topic;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusMainTest
@TestProfile(TopicCommandTest.TestConfig.class)
class TopicCommandTest {

    private static Path tempConfigDir;
    private static int kafkaBootstrapPort;
    private static String kafkaBootstrapServers;

    public static class TestConfig implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "xdg.config.home", tempConfigDir.toString(),
                    "quarkus.kafka.devservices.port", String.valueOf(kafkaBootstrapPort)
            );
        }
    }

    QuarkusMainLauncher launcher;

    @BeforeAll
    static void initialize() {
        try {
            tempConfigDir = Files.createTempDirectory("clik-integration-test");
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                kafkaBootstrapPort = serverSocket.getLocalPort();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            kafkaBootstrapServers = "localhost:" + kafkaBootstrapPort;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;

        // Create and set a test context
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", kafkaBootstrapServers);
        launcher.launch("context", "use", "test-context");
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up all topics
        LaunchResult listResult = launcher.launch("topic", "list", "-o", "name");
        if (listResult.exitCode() == 0 && !listResult.getOutput().contains("No topics found")) {
            for (String topic : listResult.getOutputStream().stream().map(String::strip).toList()) {
                if (!topic.isEmpty()) {
                    launcher.launch("topic", "delete", topic, "--force");
                }
            }
        }

        // Clean up config directory
        Path configDir = tempConfigDir.resolve("clik");
        if (Files.exists(configDir)) {
            Files.walk(configDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    @Test
    void testCreateTopic() {
        LaunchResult result = launcher.launch("topic", "create", "test-topic");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Topic \"test-topic\" created"));

        // Verify topic was created
        LaunchResult listResult = launcher.launch("topic", "list", "-o", "name");
        assertTrue(listResult.getOutput().contains("test-topic"));
    }

    @Test
    void testCreateTopicWithPartitions() {
        LaunchResult result = launcher.launch("topic", "create", "test-topic", "--partitions", "5");
        assertEquals(0, result.exitCode());

        // Verify partition count
        LaunchResult describeResult = launcher.launch("topic", "describe", "test-topic");
        assertTrue(describeResult.getOutput().contains("Partitions: 5"));
    }

    @Test
    void testCreateTopicWithConfig() {
        LaunchResult result = launcher.launch("topic", "create", "test-topic", "--config", "retention.ms=3600000");
        assertEquals(0, result.exitCode(), () -> {
            return """
                   Output: %s
                   Error: %s""".formatted(result.getOutput(), result.getErrorOutput());
        });

        // Verify config
        LaunchResult describeResult = launcher.launch("topic", "describe", "test-topic");
        assertTrue(describeResult.getOutput().contains("retention.ms"));
        assertTrue(describeResult.getOutput().contains("3600000"));
    }

    @Test
    void testCreateTopicAlreadyExists() {
        launcher.launch("topic", "create", "duplicate-topic");

        LaunchResult result = launcher.launch("topic", "create", "duplicate-topic");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("already exists"));
    }

    @Test
    void testListTopicsEmpty() {
        LaunchResult result = launcher.launch("topic", "list");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("No topics found"));
    }

    @Test
    void testListTopicsTable() {
        launcher.launch("topic", "create", "topic1", "--partitions", "3");
        launcher.launch("topic", "create", "topic2", "--partitions", "5");

        LaunchResult result = launcher.launch("topic", "list");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("topic1"));
        assertTrue(output.contains("topic2"));
        assertTrue(output.contains("3"));
        assertTrue(output.contains("5"));
    }

    @Test
    void testListTopicsNameFormat() {
        launcher.launch("topic", "create", "alpha");
        launcher.launch("topic", "create", "beta");
        launcher.launch("topic", "create", "gamma");

        LaunchResult result = launcher.launch("topic", "list", "-o", "name");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("alpha"));
        assertTrue(output.contains("beta"));
        assertTrue(output.contains("gamma"));
    }

    @Test
    void testListTopicsJsonFormat() {
        launcher.launch("topic", "create", "json-topic", "--partitions", "2");

        LaunchResult result = launcher.launch("topic", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"name\""));
        assertTrue(output.contains("\"json-topic\""));
        assertTrue(output.contains("\"partitions\""));
        assertTrue(output.contains("2"));
    }

    @Test
    void testListTopicsYamlFormat() {
        launcher.launch("topic", "create", "yaml-topic", "--partitions", "3");

        LaunchResult result = launcher.launch("topic", "list", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("name:"));
        assertTrue(output.contains("yaml-topic"));
        assertTrue(output.contains("partitions:"));
        assertTrue(output.contains("3"));
    }

    @Test
    void testDescribeTopic() {
        launcher.launch("topic", "create", "describe-test", "--partitions", "4", "--replication-factor", "1");

        LaunchResult result = launcher.launch("topic", "describe", "describe-test");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Topic: describe-test"));
        assertTrue(output.contains("Partitions: 4"));
        assertTrue(output.contains("Replication Factor: 1"));
        assertTrue(output.contains("Partition Details:"));
    }

    @Test
    void testDescribeTopicNotFound() {
        LaunchResult result = launcher.launch("topic", "describe", "nonexistent-topic");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("not found"));
    }

    @Test
    void testDescribeTopicJsonFormat() {
        launcher.launch("topic", "create", "json-describe");

        LaunchResult result = launcher.launch("topic", "describe", "json-describe", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"name\""));
        assertTrue(output.contains("\"json-describe\""));
        assertTrue(output.contains("\"partitions\""));
        assertTrue(output.contains("\"partitionDetails\""));
    }

    @Test
    void testDeleteTopic() {
        launcher.launch("topic", "create", "delete-test");

        LaunchResult result = launcher.launch("topic", "delete", "delete-test", "--force");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Topic \"delete-test\" deleted"));

        // Verify topic was deleted
        LaunchResult listResult = launcher.launch("topic", "list", "-o", "name");
        assertFalse(listResult.getOutput().contains("delete-test"));
    }

    @Test
    void testDeleteMultipleTopics() {
        launcher.launch("topic", "create", "delete1");
        launcher.launch("topic", "create", "delete2");
        launcher.launch("topic", "create", "delete3");

        LaunchResult result = launcher.launch("topic", "delete", "delete1", "delete2", "delete3", "--force");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("3 topics deleted"));

        // Verify topics were deleted
        LaunchResult listResult = launcher.launch("topic", "list", "-o", "name");
        String output = listResult.getOutput();
        assertFalse(output.contains("delete1"));
        assertFalse(output.contains("delete2"));
        assertFalse(output.contains("delete3"));
    }

    @Test
    void testAlterTopicConfig() {
        launcher.launch("topic", "create", "alter-test", "--config", "retention.ms=86400000");

        LaunchResult result = launcher.launch("topic", "alter", "alter-test", "--config", "retention.ms=172800000");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Topic \"alter-test\" configuration altered"));

        // Verify the config was altered
        LaunchResult describeResult = launcher.launch("topic", "describe", "alter-test");
        assertTrue(describeResult.getOutput().contains("retention.ms"));
        assertTrue(describeResult.getOutput().contains("172800000"));
    }

    @Test
    void testAlterTopicMultipleConfigs() {
        launcher.launch("topic", "create", "multi-config-test");

        LaunchResult result = launcher.launch("topic", "alter", "multi-config-test",
                "--config", "retention.ms=86400000",
                "--config", "compression.type=snappy");
        assertEquals(0, result.exitCode());

        // Verify both configs were set
        LaunchResult describeResult = launcher.launch("topic", "describe", "multi-config-test");
        String output = describeResult.getOutput();
        assertTrue(output.contains("retention.ms"));
        assertTrue(output.contains("86400000"));
        assertTrue(output.contains("compression.type"));
        assertTrue(output.contains("snappy"));
    }

    @Test
    void testAlterTopicDeleteConfig() {
        launcher.launch("topic", "create", "delete-config-test",
                "--config", "retention.ms=86400000",
                "--config", "compression.type=snappy");

        LaunchResult result = launcher.launch("topic", "alter", "delete-config-test",
                "--delete-config", "compression.type");
        assertEquals(0, result.exitCode());

        // Verify the config was deleted
        LaunchResult describeResult = launcher.launch("topic", "describe", "delete-config-test");
        String output = describeResult.getOutput();
        assertTrue(output.contains("retention.ms"));
        assertFalse(output.contains("compression.type"));
    }

    @Test
    void testAlterTopicSetAndDelete() {
        launcher.launch("topic", "create", "set-and-delete-test",
                "--config", "retention.ms=86400000",
                "--config", "max.message.bytes=2000000");

        LaunchResult result = launcher.launch("topic", "alter", "set-and-delete-test",
                "--config", "compression.type=lz4",
                "--delete-config", "max.message.bytes");
        assertEquals(0, result.exitCode());

        // Verify changes
        LaunchResult describeResult = launcher.launch("topic", "describe", "set-and-delete-test");
        String output = describeResult.getOutput();
        assertTrue(output.contains("retention.ms"));
        assertTrue(output.contains("compression.type"));
        assertTrue(output.contains("lz4"));
        assertFalse(output.contains("max.message.bytes"));
    }

    @Test
    void testAlterTopicNotFound() {
        LaunchResult result = launcher.launch("topic", "alter", "nonexistent-topic",
                "--config", "retention.ms=86400000");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("not found"));
    }

    @Test
    void testAlterTopicNoOptions() {
        launcher.launch("topic", "create", "no-options-test");

        LaunchResult result = launcher.launch("topic", "alter", "no-options-test");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("At least one --config or --delete-config option must be specified"));
    }

    @Test
    void testAlterTopicInvalidConfigFormat() {
        launcher.launch("topic", "create", "invalid-config-test");

        LaunchResult result = launcher.launch("topic", "alter", "invalid-config-test",
                "--config", "invalid-format");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid config format"));
    }

    @Test
    void testCreateTopicNoContext() {
        // Delete the context first to ensure no context is set
        launcher.launch("context", "delete", "test-context", "--force");

        LaunchResult result = launcher.launch("topic", "create", "no-context-topic");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }
}
