package io.streamshub.clik.command.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.test.ClikMainTestBase;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class ClusterCommandTest extends ClikMainTestBase {

    private AtomicBoolean initialized = new AtomicBoolean(false);

    QuarkusMainLauncher launcher;
    ContextService contextService;

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;

        if (initialized.compareAndSet(false, true)) {
            // Run the application to trigger the startup of the devservices Kafka instance
            launcher.launch();
        }

        this.contextService = new ContextService(xdgConfigHome().toString());

        // Create and set a test context
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", kafkaBootstrapServers())
                .build(), false);
        contextService.setCurrentContext("test-context");
    }

    @Test
    void testDescribeClusterTable() {
        LaunchResult result = launcher.launch("cluster", "describe");
        assertEquals(0, result.exitCode(), () -> {
            return """
                   Output: %s
                   Error: %s""".formatted(result.getOutput(), result.getErrorOutput());
        });

        String output = result.getOutput();
        assertTrue(output.contains("Cluster ID:"), "Output should contain cluster ID");
        assertTrue(output.contains("Controller ID:"), "Output should contain controller ID");
        assertTrue(output.contains("Nodes:"), "Output should contain node count");
        assertTrue(output.contains("Cluster Nodes:"), "Output should contain cluster nodes header");
        assertTrue(output.contains("ID"), "Output should contain ID column header");
        assertTrue(output.contains("HOST"), "Output should contain HOST column header");
        assertTrue(output.contains("PORT"), "Output should contain PORT column header");
        assertTrue(output.contains("ROLE"), "Output should contain ROLE column header");
    }

    @Test
    void testDescribeClusterJson() {
        LaunchResult result = launcher.launch("cluster", "describe", "-o", "json");
        assertEquals(0, result.exitCode(), () -> {
            return """
                   Output: %s
                   Error: %s""".formatted(result.getOutput(), result.getErrorOutput());
        });

        String output = result.getOutput();
        assertTrue(output.contains("\"clusterId\""), "JSON output should contain clusterId field");
        assertTrue(output.contains("\"controllerId\""), "JSON output should contain controllerId field");
        assertTrue(output.contains("\"nodes\""), "JSON output should contain nodes array");
        assertTrue(output.contains("\"id\""), "JSON output should contain node id field");
        assertTrue(output.contains("\"host\""), "JSON output should contain node host field");
        assertTrue(output.contains("\"port\""), "JSON output should contain node port field");
        assertTrue(output.contains("\"role\""), "JSON output should contain node role field");
    }

    @Test
    void testDescribeClusterYaml() {
        LaunchResult result = launcher.launch("cluster", "describe", "-o", "yaml");
        assertEquals(0, result.exitCode(), () -> {
            return """
                   Output: %s
                   Error: %s""".formatted(result.getOutput(), result.getErrorOutput());
        });

        String output = result.getOutput();
        assertTrue(output.contains("clusterId:"), "YAML output should contain clusterId field");
        assertTrue(output.contains("controllerId:"), "YAML output should contain controllerId field");
        assertTrue(output.contains("nodes:"), "YAML output should contain nodes list");
        assertTrue(output.contains("id:"), "YAML output should contain node id field");
        assertTrue(output.contains("host:"), "YAML output should contain node host field");
        assertTrue(output.contains("port:"), "YAML output should contain node port field");
        assertTrue(output.contains("role:"), "YAML output should contain node role field");
    }

    @Test
    void testDescribeClusterInvalidFormat() {
        LaunchResult result = launcher.launch("cluster", "describe", "-o", "xml");
        assertEquals(1, result.exitCode(), "Invalid format should return exit code 1");
        String errorOutput = result.getErrorOutput();
        assertTrue(errorOutput.contains("Unknown output format"), "Error should mention unknown format");
        assertTrue(errorOutput.contains("table, yaml, json"), "Error should list valid formats");
    }

    @Test
    void testDescribeClusterMissingContext() {
        // Remove the current context
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("cluster", "describe");
        assertEquals(1, result.exitCode(), "Missing context should return exit code 1");
        assertTrue(result.getErrorOutput().contains("No current context set"),
                "Error should mention missing context");
    }

    @Test
    void testClusterCommandHasDescribeSubcommand() {
        CommandLine cmd = new CommandLine(new ClusterCommand());
        assertNotNull(cmd.getSubcommands().get("describe"),
                "ClusterCommand should have describe subcommand");
    }

    @Test
    void testClusterCommandHasHelpSubcommand() {
        CommandLine cmd = new CommandLine(new ClusterCommand());
        assertNotNull(cmd.getSubcommands().get("help"),
                "ClusterCommand should have help subcommand");
    }
}
