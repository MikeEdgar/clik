package io.streamshub.clik.command.topic;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;
import io.streamshub.clik.test.TestRecordProducer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class TopicCommandTest extends ClikMainTestBase implements TestRecordProducer {

    private AtomicBoolean initialized = new AtomicBoolean(false);

    QuarkusMainLauncher launcher;
    ContextService contextService;
    TopicService topicService;

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;

        if (initialized.compareAndSet(false, true)) {
            // Run the application to trigger the startup of the devservices Kafka instance
            launcher.launch();
        }

        this.contextService = new ContextService(xdgConfigHome().toString());
        this.topicService = new TopicService();

        // Create and set a test context
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", kafkaBootstrapServers())
                .build(), false);
        contextService.setCurrentContext("test-context");
    }

    @Test
    void testCreateTopic() throws Exception {
        LaunchResult result = launcher.launch("topic", "create", "test-topic");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Topic \"test-topic\" created"));

        // Verify topic was created
        var topicInfo = topicService.describeTopic(admin(), "test-topic");
        assertNotNull(topicInfo);
        assertEquals("test-topic", topicInfo.name());
    }

    @Test
    void testCreateTopicWithPartitions() throws Exception {
        LaunchResult result = launcher.launch("topic", "create", "test-topic", "--partitions", "5");
        assertEquals(0, result.exitCode());

        // Verify partition count
        var topicInfo = topicService.describeTopic(admin(), "test-topic");
        assertNotNull(topicInfo);
        assertEquals(5, topicInfo.partitions());
        assertEquals(5, topicInfo.partitionDetails().size());
    }

    @Test
    void testCreateTopicWithConfig() throws Exception {
        LaunchResult result = launcher.launch("topic", "create", "test-topic", "--config", "retention.ms=3600000");
        assertEquals(0, result.exitCode(), () -> {
            return """
                   Output: %s
                   Error: %s""".formatted(result.getOutput(), result.getErrorOutput());
        });

        // Verify config
        var topicInfo = topicService.describeTopic(admin(), "test-topic");
        assertNotNull(topicInfo);
        assertEquals("3600000", topicInfo.config().get("retention.ms"));
    }

    @Test
    void testCreateTopicAlreadyExists() throws Exception {
        topicService.createTopic(admin(), "duplicate-topic", 1, 1, Collections.emptyMap());

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
    void testListTopicsTable() throws Exception {
        topicService.createTopic(admin(), "topic1", 3, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "topic2", 5, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "list");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("topic1"));
        assertTrue(output.contains("topic2"));
        assertTrue(output.contains("3"));
        assertTrue(output.contains("5"));
    }

    @Test
    void testListTopicsNameFormat() throws Exception {
        topicService.createTopic(admin(), "alpha", 1, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "beta", 1, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "gamma", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "list", "-o", "name");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("alpha"));
        assertTrue(output.contains("beta"));
        assertTrue(output.contains("gamma"));
    }

    @Test
    void testListTopicsJsonFormat() throws Exception {
        topicService.createTopic(admin(), "json-topic", 2, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"name\""));
        assertTrue(output.contains("\"json-topic\""));
        assertTrue(output.contains("\"partitions\""));
        assertTrue(output.contains("2"));
    }

    @Test
    void testListTopicsYamlFormat() throws Exception {
        topicService.createTopic(admin(), "yaml-topic", 3, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "list", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("name:"));
        assertTrue(output.contains("yaml-topic"));
        assertTrue(output.contains("partitions:"));
        assertTrue(output.contains("3"));
    }

    @Test
    void testDescribeTopic() throws Exception {
        topicService.createTopic(admin(), "describe-test", 4, 1, Collections.emptyMap());

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
    void testDescribeTopicJsonFormat() throws Exception {
        topicService.createTopic(admin(), "json-describe", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "describe", "json-describe", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"name\""));
        assertTrue(output.contains("\"json-describe\""));
        assertTrue(output.contains("\"partitions\""));
        assertTrue(output.contains("\"partitionDetails\""));
    }

    @ParameterizedTest
    @CsvSource({
        "'earliest,latest'        , max-timestamp, 0, 10,  9",
        "'-2,-1'                  , -3           , 0, 10,  9", // special value numerics for earliest/latest/max-timestamp
        "'P102D,PT1S'             , P12D         , 0, -1,  9",
        "'earliest,earliest-local', latest-tiered, 0,  0, -1",
        "-2                       , '-4,-5'      , 0,  0, -1", // special value numerics for earliest/earliest-local/latest-tiered
        "'2025-01-01T00:00:00Z'   , '0,1'        , 0,  0,  0", // 0 & 1 are 1970-01-01T00:00:00Z and 1970-01-01T00:00:00.001Z
    })
    void testDescribeTopicWithOffsets(String offsets1, String offsets2, String expected1, String expected2, String expected3) throws Exception {
        topicService.createTopic(admin(), "describe-offsets", 10, 1, Collections.emptyMap());
        var baseTime = Instant.now().minus(Duration.ofDays(101));

        produceMessagesWithTimestamps("describe-offsets", 100,
                baseTime.toEpochMilli(),
                Duration.ofDays(1).toMillis());

        // Retry for several seconds. The timeindex may not immediately be updated after producing messages
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            LaunchResult result = launcher.launch(
                    "topic", "describe", "describe-offsets",
                    "--offsets", offsets1,
                    "--offsets", offsets2
            );
            assertEquals(0, result.exitCode());
            List<String> output = result.getOutputStream();
            int start = output.indexOf("Partition Details:") + 2;
            int p = 0;

            for (int l = start; l < output.size(); l++) {
                String partitionLine = output.get(l);
                assertTrue(partitionLine.matches(
                        "^"         // start of line
                        + "\\s+"    // variable whitespace
                        + (p++)     // partition#
                        + "\\s+"    // variable whitespace
                        + "1"       // leader
                        + "\\s+"    // variable whitespace
                        + "\\[1\\]" // replicas
                        + "\\s+"    // variable whitespace
                        + "\\[1\\]" // ISR
                        + "\\s+"    // variable whitespace
                        + expected1 // expected offset 1
                        + "\\s+"    // variable whitespace
                        + expected2 // expected offset 2
                        + "\\s+"    // variable whitespace
                        + expected3 // expected offset 3
                        + "\\s*"    // maybe variable whitespace
                        + "$"       // end of line
                        ), () -> "Partition line did not match: '" + partitionLine +
                            System.lineSeparator() + "'. Full output: " + result.getOutput());
            }
        });
    }

    @Test
    void testDeleteTopic() throws Exception {
        topicService.createTopic(admin(), "delete-test", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "delete", "delete-test", "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Topic \"delete-test\" deleted"));

        // Verify topic was deleted
        var topics = topicService.listTopics(admin(), false);
        assertFalse(topics.contains("delete-test"));
    }

    @Test
    void testDeleteMultipleTopics() throws Exception {
        topicService.createTopic(admin(), "delete1", 1, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "delete2", 1, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "delete3", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "delete", "delete1", "delete2", "delete3", "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("3 topics deleted"));

        // Verify topics were deleted
        var topics = topicService.listTopics(admin(), false);
        assertFalse(topics.contains("delete1"));
        assertFalse(topics.contains("delete2"));
        assertFalse(topics.contains("delete3"));
    }

    @Test
    void testAlterTopicConfig() throws Exception {
        topicService.createTopic(admin(), "alter-test", 1, 1, Map.of("retention.ms", "86400000"));

        LaunchResult result = launcher.launch("topic", "alter", "alter-test", "--config", "retention.ms=172800000");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Topic \"alter-test\" configuration altered"));

        // Verify the config was altered
        var topicInfo = topicService.describeTopic(admin(), "alter-test");
        assertNotNull(topicInfo);
        assertEquals("172800000", topicInfo.config().get("retention.ms"));
    }

    @Test
    void testAlterTopicMultipleConfigs() throws Exception {
        topicService.createTopic(admin(), "multi-config-test", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "multi-config-test",
                "--config", "retention.ms=86400000",
                "--config", "compression.type=snappy");
        assertEquals(0, result.exitCode());

        // Verify both configs were set
        var topicInfo = topicService.describeTopic(admin(), "multi-config-test");
        assertNotNull(topicInfo);
        assertEquals("86400000", topicInfo.config().get("retention.ms"));
        assertEquals("snappy", topicInfo.config().get("compression.type"));
    }

    @Test
    void testAlterTopicDeleteConfig() throws Exception {
        topicService.createTopic(admin(), "delete-config-test", 1, 1,
                Map.of("retention.ms", "86400000", "compression.type", "snappy"));

        LaunchResult result = launcher.launch("topic", "alter", "delete-config-test",
                "--delete-config", "compression.type");
        assertEquals(0, result.exitCode());

        // Verify the config was deleted
        var topicInfo = topicService.describeTopic(admin(), "delete-config-test");
        assertNotNull(topicInfo);
        assertTrue(topicInfo.config().containsKey("retention.ms"));
        assertFalse(topicInfo.config().containsKey("compression.type"));
    }

    @Test
    void testAlterTopicSetAndDelete() throws Exception {
        topicService.createTopic(admin(), "set-and-delete-test", 1, 1,
                Map.of("retention.ms", "86400000", "max.message.bytes", "2000000"));

        LaunchResult result = launcher.launch("topic", "alter", "set-and-delete-test",
                "--config", "compression.type=lz4",
                "--delete-config", "max.message.bytes");
        assertEquals(0, result.exitCode());

        // Verify changes
        var topicInfo = topicService.describeTopic(admin(), "set-and-delete-test");
        assertNotNull(topicInfo);
        assertTrue(topicInfo.config().containsKey("retention.ms"));
        assertEquals("lz4", topicInfo.config().get("compression.type"));
        assertFalse(topicInfo.config().containsKey("max.message.bytes"));
    }

    @Test
    void testAlterTopicNotFound() {
        LaunchResult result = launcher.launch("topic", "alter", "nonexistent-topic",
                "--config", "retention.ms=86400000");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("not found"));
    }

    @Test
    void testAlterTopicNoOptions() throws Exception {
        topicService.createTopic(admin(), "no-options-test", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "no-options-test");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("At least one --config, --delete-config, or --partitions option must be specified"));
    }

    @Test
    void testAlterTopicInvalidConfigFormat() throws Exception {
        topicService.createTopic(admin(), "invalid-config-test", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "invalid-config-test",
                "--config", "invalid-format");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid config format"));
    }

    @Test
    void testAlterTopicIncreasePartitions() throws Exception {
        topicService.createTopic(admin(), "partition-test", 3, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "partition-test", "--partitions", "6");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("partitions increased from 3 to 6"));

        // Verify partition count was increased
        var topicInfo = topicService.describeTopic(admin(), "partition-test");
        assertNotNull(topicInfo);
        assertEquals(6, topicInfo.partitions());
    }

    @Test
    void testAlterTopicDecreasePartitionsError() throws Exception {
        topicService.createTopic(admin(), "decrease-test", 5, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "decrease-test", "--partitions", "3");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("must be greater than current count"));
        assertTrue(result.getErrorOutput().contains("does not support decreasing"));
    }

    @Test
    void testAlterTopicSamePartitionCountError() throws Exception {
        topicService.createTopic(admin(), "same-partition-test", 4, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "same-partition-test", "--partitions", "4");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("must be greater than current count"));
    }

    @Test
    void testAlterTopicPartitionsAndConfig() throws Exception {
        topicService.createTopic(admin(), "combined-test", 2, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "combined-test",
            "--partitions", "5",
            "--config", "retention.ms=3600000");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("partitions increased from 2 to 5"));
        assertTrue(result.getOutput().contains("configuration altered"));

        // Verify both partition count and config were changed
        var topicInfo = topicService.describeTopic(admin(), "combined-test");
        assertNotNull(topicInfo);
        assertEquals(5, topicInfo.partitions());
        assertEquals("3600000", topicInfo.config().get("retention.ms"));
    }

    @Test
    void testAlterTopicPartitionsOnly() throws Exception {
        topicService.createTopic(admin(), "partitions-only-test", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("topic", "alter", "partitions-only-test", "--partitions", "8");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("partitions increased from 1 to 8"));
        assertFalse(result.getOutput().contains("configuration"));
    }

    @Test
    void testCreateTopicNoContext() {
        // Delete the context first to ensure no context is set
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("topic", "create", "no-context-topic");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }
}
