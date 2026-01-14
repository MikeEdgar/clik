package io.streamshub.clik.command.context;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.test.ClikMainTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class ContextCommandTest extends ClikMainTestBase {

    QuarkusMainLauncher launcher;
    ContextService contextService;

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;
        this.contextService = new ContextService(xdgConfigHome().toString());
    }

    @Test
    @Launch({"context", "create", "test-context", "--bootstrap-servers", "localhost:9092"})
    void testCreateContextWithBootstrapServers(LaunchResult result) {
        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" created.", result.getOutput().trim());

        // Verify context was created by listing
        assertEquals(List.of("test-context"), contextService.listContexts());
    }

    @Test
    @Launch(value = {"context", "create", "my context", "--bootstrap-servers", "localhost:9092"}, exitCode = 1)
    void testCreateContextWithInvalidName(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid context name"));

        // Verify context was not created
        assertTrue(contextService.listContexts().isEmpty());
    }

    @Test
    @Launch({"context", "create", "test-context", "--bootstrap-servers", "localhost:9092",
            "--security-protocol", "SASL_SSL",
            "--sasl-mechanism", "SCRAM-SHA-512",
            "--property", "consumer.group.id=test-group",
            "--property", "producer.acks=all"})
    void testCreateContextWithProperties(LaunchResult result) {
        assertEquals(0, result.exitCode());

        // Verify configuration
        var context = contextService.loadContext("test-context");
        assertNotNull(context);
        assertEquals(Map.of(
                "bootstrap.servers", "localhost:9092",
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "SCRAM-SHA-512"
            ), context.getCommon());
        assertTrue(context.getAdmin().isEmpty());
        assertEquals(Map.of("group.id", "test-group"), context.getConsumer());
        assertEquals(Map.of("acks", "all"), context.getProducer());
    }

    @Test
    @Launch({"context", "list"})
    void testListContextsEmpty(LaunchResult result) {
        assertEquals(0, result.exitCode());
        assertEquals("No contexts found.", result.getOutput().trim());
    }

    @Test
    void testListContextsTable() {
        // Create contexts using the service
        contextService.createContext("dev", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.createContext("prod", ContextConfig.builder()
                .addCommon("bootstrap.servers", "prod.kafka:9092")
                .addCommon("security.protocol", "SASL_SSL")
                .build(), false);

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
        // Create contexts using the service
        contextService.createContext("dev", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.createContext("prod", ContextConfig.builder()
                .addCommon("bootstrap.servers", "prod.kafka:9092")
                .build(), false);

        // List contexts in name format
        LaunchResult result = launcher.launch("context", "list", "-o", "name");

        assertEquals(0, result.exitCode());
        assertEquals(List.of("dev", "prod"), result.getOutputStream().stream().map(String::strip).toList());
    }

    @Test
    void testListContextsYamlFormat() throws Exception {
        // Create contexts using the service
        contextService.createContext("dev", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.createContext("prod", ContextConfig.builder()
                .addCommon("bootstrap.servers", "prod.kafka:9092")
                .addCommon("security.protocol", "SASL_SSL")
                .build(), false);

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
        // Create contexts using the service
        contextService.createContext("dev", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.createContext("prod", ContextConfig.builder()
                .addCommon("bootstrap.servers", "prod.kafka:9092")
                .addCommon("security.protocol", "SASL_SSL")
                .build(), false);

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
        // Create context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);

        // Use the context
        LaunchResult result = launcher.launch("context", "use", "test-context");

        assertEquals(0, result.exitCode());
        assertEquals("Switched to context \"test-context\".", result.getOutput().trim());

        // Verify current context was set
        assertEquals("test-context", contextService.getCurrentContext().orElse(null));
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
        // Create and set context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.setCurrentContext("test-context");

        // Get current context
        LaunchResult result = launcher.launch("context", "current");

        assertEquals(0, result.exitCode());
        assertEquals("test-context", result.getOutput().trim());
    }

    @Test
    void testCurrentContextWithConfig() {
        // Create and set context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addCommon("security.protocol", "PLAINTEXT")
                .build(), false);
        contextService.setCurrentContext("test-context");

        // Get current context with config
        LaunchResult result = launcher.launch("context", "current", "--describe");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Current context: test-context"));
        assertTrue(output.contains("bootstrap.servers"));
        assertTrue(output.contains("localhost:9092"));
    }

    @Test
    void testDeleteContext() {
        // Create context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);

        // Delete the context
        LaunchResult result = launcher.launch("context", "delete", "test-context", "--yes");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" deleted.", result.getOutput().trim());

        // Verify context was deleted
        assertTrue(contextService.listContexts().isEmpty());
    }

    @Test
    void testDeleteCurrentContextClearsIt() {
        // Create and set context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.setCurrentContext("test-context");

        // Delete the current context
        LaunchResult result = launcher.launch("context", "delete", "test-context", "--yes");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" deleted.", result.getOutput().trim());

        // Verify current context was cleared
        assertTrue(contextService.getCurrentContext().isEmpty());
    }

    @Test
    @Launch(value = {"context", "delete", "nonexistent", "--yes"}, exitCode = 1)
    void testDeleteContextNotFound(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("does not exist"));
    }

    @Test
    void testDescribeContext() {
        // Create context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addConsumer("group.id", "test-group")
                .addProducer("acks", "all")
                .build(), false);

        // Describe the context
        LaunchResult result = launcher.launch("context", "describe", "test-context");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("bootstrap.servers"));
        assertTrue(output.contains("localhost:9092"));
        assertTrue(output.contains("group.id"));
        assertTrue(output.contains("test-group"));
    }

    @Test
    @Launch(value = {"context", "describe", "nonexistent"}, exitCode = 1)
    void testDescribeContextNotFound(LaunchResult result) {
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("does not exist"));
    }

    @Test
    void testDescribeContextPropertiesFormat() {
        // Create context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addConsumer("group.id", "test-group")
                .build(), false);

        // Describe context in properties format
        LaunchResult result = launcher.launch("context", "describe", "test-context", "-o", "properties");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("bootstrap.servers=localhost:9092"));
        assertTrue(output.contains("consumer.group.id=test-group"));
    }

    @Test
    void testDescribeContextJsonFormat() {
        // Create context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);

        // Describe context in JSON format
        LaunchResult result = launcher.launch("context", "describe", "test-context", "-o", "json");

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
        // Create first context using the service
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9091")
                .build(), false);

        // Overwrite with new bootstrap servers
        LaunchResult result = launcher.launch("context", "create", "test-context",
                "--bootstrap-servers", "localhost:9092", "--overwrite");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"test-context\" created.", result.getOutput().trim());

        // Verify the context was overwritten
        var context = contextService.loadContext("test-context");
        assertEquals(context.getCommon().get("bootstrap.servers"), "localhost:9092");
    }

    @Test
    void testRenameContext() {
        // Create context using the service
        contextService.createContext("old-name", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);

        // Rename the context
        LaunchResult result = launcher.launch("context", "rename", "old-name", "new-name");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"old-name\" renamed to \"new-name\".", result.getOutput().trim());

        // Verify only new context exists
        assertEquals(List.of("new-name"), contextService.listContexts());

        // Verify configuration was preserved
        var context = contextService.loadContext("new-name");
        assertEquals("localhost:9092", context.getCommon().get("bootstrap.servers"));
    }

    @Test
    void testRenameCurrentContext() {
        // Create and set context using the service
        contextService.createContext("old-name", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.setCurrentContext("old-name");

        // Rename the current context
        LaunchResult result = launcher.launch("context", "rename", "old-name", "new-name");

        assertEquals(0, result.exitCode());
        assertEquals("Context \"old-name\" renamed to \"new-name\".", result.getOutput().trim());

        // Verify current context was updated
        assertEquals("new-name", contextService.getCurrentContext().orElse(null));
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
        contextService.createContext("context1", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);
        contextService.createContext("context2", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9093")
                .build(), false);

        // Try to rename context1 to context2
        LaunchResult result = launcher.launch("context", "rename", "context1", "context2");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("already exists"));
    }

    @Test
    void testRenameContextInvalidNewName() {
        // Create context first
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build(), false);

        // Try to rename with invalid name
        LaunchResult result = launcher.launch("context", "rename", "test-context", "invalid name");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid context name"));
    }

    @Test
    void testCreateContextWithVerifySuccess() {
        // Create context with --verify using dev services Kafka
        LaunchResult result = launcher.launch("context", "create", "verified-context",
                "--bootstrap-servers", kafkaBootstrapServers(), "--verify");

        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Verifying connection to Kafka cluster"));
        assertTrue(output.contains("Connection verified successfully"));
        assertTrue(output.contains("Context \"verified-context\" created"));

        // Verify context was actually created
        assertEquals(List.of("verified-context"), contextService.listContexts());
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
        assertEquals(List.of("invalid-context"), contextService.listContexts());
    }
}
