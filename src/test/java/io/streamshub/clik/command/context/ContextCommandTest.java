package io.streamshub.clik.command.context;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ContextCommandTest.TestConfig.class)
class ContextCommandTest {

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
    }

    @AfterEach
    void tearDown() throws IOException {
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
    @Launch({"context", "create", "test-context", "--bootstrap-servers", "localhost:9092"})
    void testCreateContextWithBootstrapServers(LaunchResult result) {
        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" created.", result.getOutput().trim());

        // Verify context was created by listing
        LaunchResult listResult = launcher.launch("context", "list", "-o", "name");
        assertEquals("test-context", listResult.getOutput().trim());
    }

    @Test
    @Launch(value = {"context", "create", "my context", "--bootstrap-servers", "localhost:9092"}, exitCode = 1)
    void testCreateContextWithInvalidName(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid context name"));

        // Verify context was not created
        LaunchResult listResult = launcher.launch("context", "list", "-o", "name");
        assertEquals("No contexts found.", listResult.getOutput().trim());
    }

    @Test
    @Launch({"context", "create", "test-context", "--bootstrap-servers", "localhost:9092",
            "--security-protocol", "SASL_SSL",
            "--sasl-mechanism", "SCRAM-SHA-512",
            "--property", "consumer.group.id=test-group",
            "--property", "producer.acks=all"})
    void testCreateContextWithProperties(LaunchResult result) {
        assertEquals(0, result.exitCode());

        // Verify configuration by showing the context
        LaunchResult showResult = launcher.launch("context", "show", "test-context", "-o", "properties");
        String output = showResult.getOutput();
        assertTrue(output.contains("bootstrap.servers=localhost:9092"));
        assertTrue(output.contains("security.protocol=SASL_SSL"));
        assertTrue(output.contains("sasl.mechanism=SCRAM-SHA-512"));
        assertTrue(output.contains("consumer.group.id=test-group"));
        assertTrue(output.contains("producer.acks=all"));
    }

    @Test
    @Launch({"context", "list"})
    void testListContextsEmpty(LaunchResult result) {
        assertEquals(0, result.exitCode());
        assertEquals("No contexts found.", result.getOutput().trim());
    }

    @Test
    void testListContextsTable() {
        // Create contexts using CLI
        launcher.launch("context", "create", "dev", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "create", "prod", "--bootstrap-servers", "prod.kafka:9092",
                "--security-protocol", "SASL_SSL");

        // List contexts
        LaunchResult result = launcher.launch("context", "list");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("dev"));
        assertTrue(output.contains("prod"));
        assertTrue(output.contains("localhost:9092"));
        assertTrue(output.contains("SASL_SSL"));
    }

    @Test
    void testListContextsNameFormat() {
        // Create contexts using CLI
        launcher.launch("context", "create", "dev", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "create", "prod", "--bootstrap-servers", "prod.kafka:9092");

        // List contexts in name format
        LaunchResult result = launcher.launch("context", "list", "-o", "name");

        assertEquals(0, result.exitCode());
        assertEquals(List.of("dev", "prod"), result.getOutputStream().stream().map(String::strip).toList());
    }

    @Test
    void testListContextsYamlFormat() throws Exception {
        // Create contexts using CLI
        launcher.launch("context", "create", "dev", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "create", "prod", "--bootstrap-servers", "prod.kafka:9092",
                "--security-protocol", "SASL_SSL");

        // List contexts in YAML format
        LaunchResult result = launcher.launch("context", "list", "-o", "yaml");
        assertEquals(0, result.exitCode());

        // Parse actual output
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        List<Map<String, Object>> actual = yamlMapper.readValue(
            result.getOutput(),
            new TypeReference<List<Map<String, Object>>>() {}
        );

        // Load expected output from fixture
        try (InputStream is = getClass().getResourceAsStream("list-contexts-expected.yaml")) {
            List<Map<String, Object>> expected = yamlMapper.readValue(
                is,
                new TypeReference<List<Map<String, Object>>>() {}
            );

            assertEquals(expected, actual);
        }
    }

    @Test
    void testListContextsJsonFormat() throws Exception {
        // Create contexts using CLI
        launcher.launch("context", "create", "dev", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "create", "prod", "--bootstrap-servers", "prod.kafka:9092",
                "--security-protocol", "SASL_SSL");

        // List contexts in JSON format
        LaunchResult result = launcher.launch("context", "list", "-o", "json");
        assertEquals(0, result.exitCode());

        // Parse actual output
        ObjectMapper jsonMapper = new ObjectMapper();
        List<Map<String, Object>> actual = jsonMapper.readValue(
            result.getOutput(),
            new TypeReference<List<Map<String, Object>>>() {}
        );

        // Load expected output from fixture
        try (InputStream is = getClass().getResourceAsStream("list-contexts-expected.json")) {
            List<Map<String, Object>> expected = jsonMapper.readValue(
                is,
                new TypeReference<List<Map<String, Object>>>() {}
            );

            assertEquals(expected, actual);
        }
    }

    @Test
    void testUseContext() {
        // Create context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092");

        // Use the context
        LaunchResult result = launcher.launch("context", "use", "test-context");

        assertEquals(0, result.exitCode());
        assertEquals("Switched to context \"test-context\".", result.getOutput().trim());

        // Verify current context was set
        LaunchResult currentResult = launcher.launch("context", "current");
        assertEquals("test-context", currentResult.getOutput().trim());
    }

    @Test
    @Launch(value = {"context", "use", "nonexistent"}, exitCode = 1)
    void testUseContextNotFound(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("does not exist"));
    }

    @Test
    @Launch(value = {"context", "current"}, exitCode = 1)
    void testCurrentContextNotSet(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    @Test
    void testCurrentContext() {
        // Create and set context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "use", "test-context");

        // Get current context
        LaunchResult result = launcher.launch("context", "current");

        assertEquals(0, result.exitCode());
        assertEquals("test-context", result.getOutput().trim());
    }

    @Test
    void testCurrentContextWithConfig() {
        // Create and set context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092",
                "--security-protocol", "PLAINTEXT");
        launcher.launch("context", "use", "test-context");

        // Get current context with config
        LaunchResult result = launcher.launch("context", "current", "--show-config");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Current context: test-context"));
        assertTrue(output.contains("bootstrap.servers"));
        assertTrue(output.contains("localhost:9092"));
    }

    @Test
    void testDeleteContext() {
        // Create context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092");

        // Delete the context
        LaunchResult result = launcher.launch("context", "delete", "test-context", "--force");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" deleted.", result.getOutput().trim());

        // Verify context was deleted
        LaunchResult listResult = launcher.launch("context", "list", "-o", "name");
        assertEquals("No contexts found.", listResult.getOutput().trim());
    }

    @Test
    void testDeleteCurrentContextClearsIt() {
        // Create and set context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "use", "test-context");

        // Delete the current context
        LaunchResult result = launcher.launch("context", "delete", "test-context", "--force");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" deleted.", result.getOutput().trim());

        // Verify current context was cleared
        LaunchResult currentResult = launcher.launch("context", "current");
        assertEquals(1, currentResult.exitCode());
        assertTrue(currentResult.getErrorOutput().contains("No current context set"));
    }

    @Test
    @Launch(value = {"context", "delete", "nonexistent", "--force"}, exitCode = 1)
    void testDeleteContextNotFound(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("does not exist"));
    }

    @Test
    void testShowContext() {
        // Create context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092",
                "--property", "consumer.group.id=test-group",
                "--property", "producer.acks=all");

        // Show the context
        LaunchResult result = launcher.launch("context", "show", "test-context");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("bootstrap.servers"));
        assertTrue(output.contains("localhost:9092"));
        assertTrue(output.contains("group.id"));
        assertTrue(output.contains("test-group"));
    }

    @Test
    @Launch(value = {"context", "show", "nonexistent"}, exitCode = 1)
    void testShowContextNotFound(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("does not exist"));
    }

    @Test
    void testShowContextPropertiesFormat() {
        // Create context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092",
                "--property", "consumer.group.id=test-group");

        // Show context in properties format
        LaunchResult result = launcher.launch("context", "show", "test-context", "-o", "properties");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("bootstrap.servers=localhost:9092"));
        assertTrue(output.contains("consumer.group.id=test-group"));
    }

    @Test
    void testShowContextJsonFormat() {
        // Create context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092");

        // Show context in JSON format
        LaunchResult result = launcher.launch("context", "show", "test-context", "-o", "json");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"bootstrap.servers\""));
        assertTrue(output.contains("\"localhost:9092\""));
    }

    @Test
    @Launch(value = {"context", "create", "test-context"}, exitCode = 1)
    void testCreateContextMissingBootstrapServers(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("bootstrap.servers"));
    }

    @Test
    void testCreateContextWithOverwrite() {
        // Create first context using CLI
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9091");

        // Overwrite with new bootstrap servers
        LaunchResult result = launcher.launch("context", "create", "test-context",
                "--bootstrap-servers", "localhost:9092", "--overwrite");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" created.", result.getOutput().trim());

        // Verify the context was overwritten
        LaunchResult showResult = launcher.launch("context", "show", "test-context", "-o", "properties");
        assertTrue(showResult.getOutput().contains("bootstrap.servers=localhost:9092"));
        assertFalse(showResult.getOutput().contains("localhost:9091"));
    }

    @Test
    void testRenameContext() {
        // Create context using CLI
        launcher.launch("context", "create", "old-name", "--bootstrap-servers", "localhost:9092");

        // Rename the context
        LaunchResult result = launcher.launch("context", "rename", "old-name", "new-name");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"old-name\" renamed to \"new-name\".", result.getOutput().trim());

        // Verify old context no longer exists
        LaunchResult listResult = launcher.launch("context", "list", "-o", "name");
        assertFalse(listResult.getOutput().contains("old-name"));

        // Verify new context exists
        assertTrue(listResult.getOutput().contains("new-name"));

        // Verify configuration was preserved
        LaunchResult showResult = launcher.launch("context", "show", "new-name", "-o", "properties");
        assertTrue(showResult.getOutput().contains("bootstrap.servers=localhost:9092"));
    }

    @Test
    void testRenameCurrentContext() {
        // Create and set context using CLI
        launcher.launch("context", "create", "old-name", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "use", "old-name");

        // Rename the current context
        LaunchResult result = launcher.launch("context", "rename", "old-name", "new-name");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"old-name\" renamed to \"new-name\".", result.getOutput().trim());

        // Verify current context was updated
        LaunchResult currentResult = launcher.launch("context", "current");
        assertEquals("new-name", currentResult.getOutput().trim());
    }

    @Test
    @Launch(value = {"context", "rename", "nonexistent", "new-name"}, exitCode = 1)
    void testRenameContextNotFound(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("does not exist"));
    }

    @Test
    void testRenameContextToExistingName() {
        // Create two contexts
        launcher.launch("context", "create", "context1", "--bootstrap-servers", "localhost:9092");
        launcher.launch("context", "create", "context2", "--bootstrap-servers", "localhost:9093");

        // Try to rename context1 to context2
        LaunchResult result = launcher.launch("context", "rename", "context1", "context2");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("already exists"));
    }

    @Test
    void testRenameContextInvalidNewName() {
        // Create context first
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", "localhost:9092");

        // Try to rename with invalid name
        LaunchResult result = launcher.launch("context", "rename", "test-context", "invalid name");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid context name"));
    }

    @Test
    void testCreateContextWithVerifySuccess() {
        // Create context with --verify using dev services Kafka
        LaunchResult result = launcher.launch("context", "create", "verified-context",
                "--bootstrap-servers", kafkaBootstrapServers, "--verify");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Verifying connection to Kafka cluster"));
        assertTrue(output.contains("Connection verified successfully"));
        assertTrue(output.contains("Context \"verified-context\" created"));

        // Verify context was actually created
        LaunchResult listResult = launcher.launch("context", "list", "-o", "name");
        assertTrue(listResult.getOutput().contains("verified-context"));
    }

    @Test
    void testCreateContextWithVerifyFailure() {
        // Create context with --verify using invalid bootstrap servers
        LaunchResult result = launcher.launch("context", "create", "invalid-context",
                "--bootstrap-servers", "invalid-host:9092", "--verify");

        assertEquals(1, result.exitCode());
        String errorOutput = result.getErrorOutput();
        assertTrue(errorOutput.contains("Connection failed"));
        assertTrue(errorOutput.contains("Context was created but connection verification failed"));

        // Context should still be created even though verification failed
        LaunchResult listResult = launcher.launch("context", "list", "-o", "name");
        assertTrue(listResult.getOutput().contains("invalid-context"));
    }
}
