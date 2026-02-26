package io.streamshub.clik.command.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class FeatureCommandTest extends ClikMainTestBase {

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
    void testListFeaturesTable() {
        LaunchResult result = launcher.launch("feature", "list");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("FEATURE"));
        assertTrue(output.contains("metadata.version"));
        assertTrue(output.contains("STATUS"));
    }

    @Test
    void testListFeaturesYaml() {
        LaunchResult result = launcher.launch("feature", "list", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("name:"));
        assertTrue(output.contains("metadata.version"));
        assertTrue(output.contains("status:"));
    }

    @Test
    void testListFeaturesJson() {
        LaunchResult result = launcher.launch("feature", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"name\""));
        assertTrue(output.contains("\"metadata.version\""));
        assertTrue(output.contains("\"status\""));
    }

    @Test
    void testListFeaturesInvalidFormat() {
        LaunchResult result = launcher.launch("feature", "list", "-o", "invalid");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Unknown output format"));
    }

    @Test
    void testDescribeFeatureFound() {
        LaunchResult result = launcher.launch("feature", "describe", "metadata.version");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Feature: metadata.version"));
        assertTrue(output.contains("Status:"));
    }

    @Test
    void testDescribeFeatureFoundYaml() {
        LaunchResult result = launcher.launch("feature", "describe", "metadata.version", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("name:"));
        assertTrue(output.contains("metadata.version"));
        assertTrue(output.contains("status:"));
    }

    @Test
    void testDescribeFeatureFoundJson() {
        LaunchResult result = launcher.launch("feature", "describe", "metadata.version", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"name\" : \"metadata.version\""));
        assertTrue(output.contains("\"status\""));
    }

    @Test
    void testDescribeFeatureNotFound() {
        LaunchResult result = launcher.launch("feature", "describe", "invalid.feature");
        assertEquals(1, result.exitCode());
        String error = result.getErrorOutput();
        assertTrue(error.contains("not found"));
        assertTrue(error.contains("invalid.feature"));
    }

    @Test
    void testAlterFeatureMissingOptions() {
        LaunchResult result = launcher.launch("feature", "alter", "metadata.version");
        assertEquals(2, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Error") || result.getErrorOutput().contains("Missing"));
    }

    @Test
    void testAlterUpgradeValidation() {
        // Test that upgrading to an invalid level is rejected
        LaunchResult result = launcher.launch("feature", "alter", "metadata.version", "--upgrade", "999");
        assertEquals(1, result.exitCode());
        String error = result.getErrorOutput();
        assertTrue(error.contains("exceeds maximum") || error.contains("supported"));
    }

    @Test
    void testAlterDowngradeValidation() {
        // Test that downgrading to an invalid level is rejected
        LaunchResult result = launcher.launch("feature", "alter", "metadata.version", "--downgrade", "0");
        assertEquals(1, result.exitCode());
        String error = result.getErrorOutput();
        assertTrue(error.contains("below minimum") || error.contains("supported"));
    }

    @Test
    void testAlterFeatureNotFound() {
        LaunchResult result = launcher.launch("feature", "alter", "invalid.feature", "--upgrade", "1");
        assertEquals(1, result.exitCode());
        String error = result.getErrorOutput();
        assertTrue(error.contains("not found"));
        assertTrue(error.contains("invalid.feature"));
    }

    @Test
    void testAlterUpgradeDowngradeFeature() {
        // Get initial state
        LaunchResult describeInitial = launcher.launch("feature", "describe", "group.version", "-o", "json");
        assertEquals(0, describeInitial.exitCode());
        String initialState = describeInitial.getOutput();
        boolean isAtLevelOne = initialState.contains("\"finalizedMaxVersion\" : 1");

        if (!isAtLevelOne) {
            // If not at level 1, upgrade to it
            LaunchResult upgradeResult = launcher.launch("feature", "alter", "group.version", "--upgrade", "1", "-y");
            assertEquals(0, upgradeResult.exitCode(), "Upgrade failed: " + upgradeResult.getErrorOutput());
            assertTrue(upgradeResult.getOutput().contains("Upgraded feature \"group.version\" to level 1"));

            // Verify upgrade
            LaunchResult verify = launcher.launch("feature", "describe", "group.version", "-o", "json");
            assertTrue(verify.getOutput().contains("\"finalizedMaxVersion\" : 1"));
        }

        // Now downgrade back to 0
        LaunchResult downgradeResult = launcher.launch("feature", "alter", "group.version", "--downgrade", "0", "-y");
        assertEquals(0, downgradeResult.exitCode(), "Downgrade failed: " + downgradeResult.getErrorOutput());
        assertTrue(downgradeResult.getOutput().contains("Downgraded feature \"group.version\" to level 0"));

        // Verify downgrade
        LaunchResult verifyDowngrade = launcher.launch("feature", "describe", "group.version", "-o", "json");
        String downgradeState = verifyDowngrade.getOutput();
        assertTrue(downgradeState.contains("\"finalizedMaxVersion\" : 0") ||
                   downgradeState.contains("\"finalizedMinVersion\" : null"));

        // Upgrade back to 1 for next test
        launcher.launch("feature", "alter", "group.version", "--upgrade", "1", "-y");
    }

    @Test
    void testAlterDisableFeature() {
        // Get initial state and ensure we're at level 1
        LaunchResult describeInitial = launcher.launch("feature", "describe", "group.version", "-o", "json");
        if (!describeInitial.getOutput().contains("\"finalizedMaxVersion\" : 1")) {
            launcher.launch("feature", "alter", "group.version", "--upgrade", "1", "-y");
        }

        // Now disable (reset to 0)
        LaunchResult disableResult = launcher.launch("feature", "alter", "group.version", "--disable", "-y");
        assertEquals(0, disableResult.exitCode(), "Disable failed: " + disableResult.getErrorOutput());
        assertTrue(disableResult.getOutput().contains("Disabled feature \"group.version\""));

        // Verify the feature is disabled
        LaunchResult describeResult = launcher.launch("feature", "describe", "group.version", "-o", "json");
        String disabledState = describeResult.getOutput();
        assertTrue(disabledState.contains("\"finalizedMaxVersion\" : 0") ||
                   disabledState.contains("\"finalizedMaxVersion\" : null"));

        // Restore to level 1 for other tests
        launcher.launch("feature", "alter", "group.version", "--upgrade", "1", "-y");
    }
}
