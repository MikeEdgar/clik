package io.streamshub.clik.command.group;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
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
import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;
import io.streamshub.clik.test.TestRecordProducer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for consumer group management commands.
 *
 * <h2>Group Type Coverage Strategy</h2>
 * <p>
 * These tests comprehensively cover all Kafka 4.1 group types: CLASSIC, CONSUMER, SHARE, and STREAMS.
 * Most functional tests use {@code @ParameterizedTest} with {@code @CsvSource} to execute the same
 * test logic across multiple group type combinations:
 * </p>
 * <ul>
 *   <li><b>CLASSIC/CLASSIC</b> - Classic consumer group with classic protocol</li>
 *   <li><b>CLASSIC/CONSUMER</b> - Classic consumer group with consumer protocol</li>
 *   <li><b>CONSUMER</b> - Consumer group type</li>
 *   <li><b>SHARE</b> - Share group type (Kafka 4.1+)</li>
 *   <li><b>STREAMS</b> - Streams group type (limited support in Kafka 4.1)</li>
 * </ul>
 *
 * <h3>STREAMS Group Limitations</h3>
 * <p>
 * Due to known limitations in Kafka 4.1, STREAMS groups are <b>excluded</b> from describe and alter
 * operations (describeGroup and alterOffsets APIs do not support STREAMS groups). However, STREAMS
 * groups <b>are included</b> in list and delete operations, which work correctly.
 * </p>
 *
 * <h3>Test Execution Count</h3>
 * <p>
 * Parameterized tests significantly increase test coverage without code duplication:
 * </p>
 * <ul>
 *   <li>Alter commands: 6 tests × 4 group types = 24 executions</li>
 *   <li>Delete command: 1 test × 5 group types = 5 executions (includes STREAMS)</li>
 *   <li>Describe commands: 5 tests × 4 group types = 20 executions</li>
 * </ul>
 *
 * <h3>Helper Methods</h3>
 * <p>
 * Tests use {@code createConsumer(GroupType, GroupProtocol, groupId, topic)} from
 * {@link io.streamshub.clik.test.CommonTestBase} to create consumers of the appropriate type.
 * </p>
 *
 * @see io.streamshub.clik.kafka.GroupServiceTest for unit-level group type coverage
 */
@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class GroupCommandTest extends ClikMainTestBase implements TestRecordProducer {

    private AtomicBoolean initialized = new AtomicBoolean(false);

    QuarkusMainLauncher launcher;
    ContextService contextService;
    TopicService topicService;
    GroupService groupService;

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;

        if (initialized.compareAndSet(false, true)) {
            // Run the application to trigger the startup of the devservices Kafka instance
            launcher.launch();
        }

        this.contextService = new ContextService(xdgConfigHome().toString());
        this.topicService = new TopicService();
        this.groupService = new GroupService();

        // Create and set a test context
        contextService.createContext("test-context", ContextConfig.builder()
                .addCommon("bootstrap.servers", kafkaBootstrapServers())
                .build(), false);
        contextService.setCurrentContext("test-context");
    }

    @Test
    void testListGroupsEmpty() {
        LaunchResult result = launcher.launch("group", "list");
        assertEquals(0, result.exitCode());
        // Note: May have internal Kafka groups, so just verify the command works
        assertNotNull(result.getOutput());
    }

    @Test
    void testListGroupsTable() throws Exception {
        // Create test topic and consumer groups
        topicService.createTopic(admin(), "group-test-topic", 3, 1, Collections.emptyMap());

        CompletableFuture.allOf(
                createClassicConsumer("test-group-1", "group-test-topic"),
                createClassicConsumer("test-group-2", "group-test-topic")
        ).join();

        LaunchResult result = launcher.launch("group", "list");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("test-group-1"));
        assertTrue(output.contains("test-group-2"));
        assertTrue(output.contains("NAME"));
        assertTrue(output.contains("TYPE"));
        assertTrue(output.contains("STATE"));
        assertTrue(output.contains("MEMBERS"));
    }

    @Test
    void testListGroupsNameFormat() throws Exception {
        // Create test topic and consumer groups
        topicService.createTopic(admin(), "name-test-topic", 2, 1, Collections.emptyMap());

        CompletableFuture.allOf(
                createClassicConsumer("alpha-group", "name-test-topic"),
                createClassicConsumer("beta-group", "name-test-topic"),
                createClassicConsumer("gamma-group", "name-test-topic")
        ).join();

        LaunchResult result = launcher.launch("group", "list", "-o", "name");
        assertEquals(0, result.exitCode(), "Command should succeed");
        String output = result.getOutput();
        assertTrue(output.contains("alpha-group"), "Should contain alpha-group. Output: " + output);
        assertTrue(output.contains("beta-group"), "Should contain beta-group. Output: " + output);
        assertTrue(output.contains("gamma-group"), "Should contain gamma-group. Output: " + output);
    }

    @Test
    void testListGroupsJsonFormat() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "json-group-topic", 2, 1, Collections.emptyMap());
        createClassicConsumer("json-group", "json-group-topic").join();

        LaunchResult result = launcher.launch("group", "list", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"groupId\""));
        assertTrue(output.contains("\"json-group\""));
        assertTrue(output.contains("\"type\""));
        assertTrue(output.contains("\"state\""));
        assertTrue(output.contains("\"members\""));
    }

    @Test
    void testListGroupsYamlFormat() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "yaml-group-topic", 2, 1, Collections.emptyMap());
        createClassicConsumer("yaml-group", "yaml-group-topic").join();

        LaunchResult result = launcher.launch("group", "list", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("groupId:"));
        assertTrue(output.contains("yaml-group"));
        assertTrue(output.contains("type:"));
        assertTrue(output.contains("state:"));
        assertTrue(output.contains("members:"));
    }

    @Test
    void testListGroupsFilterByType() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "filter-topic", 2, 1, Collections.emptyMap());
        createConsumer(GroupType.CONSUMER, GroupProtocol.CLASSIC, "consumer-group-1", "filter-topic").join();

        // Filter by consumer type
        LaunchResult consumerResult = launcher.launch("group", "list", "--type", "classic");
        assertEquals(0, consumerResult.exitCode());
        assertTrue(consumerResult.getOutput().contains("consumer-group-1"));

        // Filter by share type (should be empty)
        LaunchResult shareResult = launcher.launch("group", "list", "--type", "share");
        assertEquals(0, shareResult.exitCode());
        assertTrue(shareResult.getOutput().contains("No groups found"));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - describe not supported for STREAMS groups
    })
    void testDescribeGroup(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "describe-topic-" + groupType.name().toLowerCase();
        String groupId = "describe-group-" + groupType.name().toLowerCase();

        // Create test topic and consumer group
        topicService.createTopic(admin(), topicName, 3, 1, Collections.emptyMap());
        createConsumer(groupType, groupProtocol, groupId, topicName).join();

        LaunchResult result = launcher.launch("group", "describe", groupId);
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Group: " + groupId));
        assertTrue(output.contains("Type:"));
        assertTrue(output.contains("State:"));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - describe not supported for STREAMS groups
    })
    void testDescribeGroupWithMembers(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "members-topic-" + groupType.name().toLowerCase();
        String groupId = "multi-member-group-" + groupType.name().toLowerCase();

        // Create test topic and multiple consumers
        topicService.createTopic(admin(), topicName, 6, 1, Collections.emptyMap());

        CompletableFuture.allOf(
                createConsumer(groupType, groupProtocol, groupId, topicName),
                createConsumer(groupType, groupProtocol, groupId, topicName),
                createConsumer(groupType, groupProtocol, groupId, topicName)
        ).join();

        LaunchResult result = launcher.launch("group", "describe", groupId);
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Group: " + groupId));
        assertTrue(output.contains("Members:"));
        assertTrue(output.contains("MEMBER ID"));
        assertTrue(output.contains("HOST"));
        assertTrue(output.contains("CLIENT ID"));
    }

    @Test
    void testDescribeGroupNotFound() {
        LaunchResult result = launcher.launch("group", "describe", "nonexistent-group");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("not found"));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - describe not supported for STREAMS groups
    })
    void testDescribeGroupJsonFormat(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "json-describe-topic-" + groupType.name().toLowerCase();
        String groupId = "json-describe-group-" + groupType.name().toLowerCase();

        // Create test topic and consumer group
        topicService.createTopic(admin(), topicName, 2, 1, Collections.emptyMap());
        createConsumer(groupType, groupProtocol, groupId, topicName).join();

        LaunchResult result = launcher.launch("group", "describe", groupId, "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"groupId\""));
        assertTrue(output.contains("\"" + groupId + "\""));
        assertTrue(output.contains("\"type\""));
        assertTrue(output.contains("\"state\""));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - describe not supported for STREAMS groups
    })
    void testDescribeGroupYamlFormat(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "yaml-describe-topic-" + groupType.name().toLowerCase();
        String groupId = "yaml-describe-group-" + groupType.name().toLowerCase();

        // Create test topic and consumer group
        topicService.createTopic(admin(), topicName, 2, 1, Collections.emptyMap());
        createConsumer(groupType, groupProtocol, groupId, topicName).join();

        LaunchResult result = launcher.launch("group", "describe", groupId, "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("groupId:"));
        assertTrue(output.contains(groupId));
        assertTrue(output.contains("type:"));
        assertTrue(output.contains("state:"));
    }

    @Test
    void testDescribeGroupNoContext() {
        // Delete the context first to ensure no context is set
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("group", "describe", "some-group");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - describe not supported for STREAMS groups
    })
    void testDescribeGroupWithOffsets(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "offset-test-topic-" + groupType.name().toLowerCase();
        String groupId = "offset-test-group-" + groupType.name().toLowerCase();

        // Create test topic
        topicService.createTopic(admin(), topicName, 2, 1, Collections.emptyMap());

        // Produce some messages
        produceMessages(topicName, 50);

        // Create consumer and consume messages
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.poll(Duration.ofSeconds(1));
        consumer.commit();

        LaunchResult result = launcher.launch("group", "describe", groupId);
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Group: " + groupId));
        assertTrue(output.contains("Topic Lag:"));
        assertTrue(output.contains("TOPIC"));
        assertTrue(output.contains("PARTITION"));
        assertTrue(output.contains("CURRENT OFFSET"));
        assertTrue(output.contains("LOG END OFFSET"));
        assertTrue(output.contains("LAG"));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        "STREAMS ,         ,", // STREAMS deletion works (only describe/alter have limitations)
    })
    void testDeleteGroup(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "delete-test-topic-" + groupType.name().toLowerCase();
        String groupId = "delete-test-group-" + groupType.name().toLowerCase();

        // Create test topic and consumer group
        topicService.createTopic(admin(), topicName, 2, 1, Collections.emptyMap());
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();

        // Close the consumer so the group becomes empty/eligible for deletion
        consumer.close();

        LaunchResult result = launcher.launch("group", "delete", groupId, "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Group \"" + groupId + "\" deleted"));

        // Verify group was deleted - there may be a small delay, hence await
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            var groups = groupService.listGroups(admin(), null);
            assertEquals(0, groups.size());
        });
    }

    @Test
    void testDeleteMultipleGroups() throws Exception {
        // Create test topic and consumer groups
        topicService.createTopic(admin(), "multi-delete-topic", 2, 1, Collections.emptyMap());
        var consumer1 = createConsumer(GroupType.CONSUMER, GroupProtocol.CLASSIC, "delete1", "multi-delete-topic");
        var consumer2 = createConsumer(GroupType.CONSUMER, GroupProtocol.CONSUMER, "delete2", "multi-delete-topic");
        var consumer3 = createConsumer(GroupType.SHARE, null, "delete3", "multi-delete-topic");
        CompletableFuture.allOf(consumer1, consumer2, consumer3).join();

        // Close all consumers so the groups become empty/eligible for deletion
        consumer1.join().close();
        consumer2.join().close();
        consumer3.join().close();

        LaunchResult result = launcher.launch("group", "delete", "delete1", "delete2", "delete3", "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("3 groups deleted"));

        // Verify groups were deleted - there may be a small delay, hence await
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            var groups = groupService.listGroups(admin(), null);
            assertEquals(0, groups.size());
        });
    }

    @Test
    void testDeleteGroupNotFound() {
        LaunchResult result = launcher.launch("group", "delete", "nonexistent-group", "--yes");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Error deleting group"));
    }

    @Test
    void testDeleteGroupNoContext() {
        // Delete the context first to ensure no context is set
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("group", "delete", "some-group", "--yes");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - alter not supported for STREAMS groups
    })
    void testAlterGroupToEarliest(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "alter-earliest-topic-" + groupType.name().toLowerCase();
        String groupId = "alter-earliest-group-" + groupType.name().toLowerCase();

        // Create topic and produce messages
        topicService.createTopic(admin(), topicName, 3, 1, Collections.emptyMap());
        produceMessages(topicName, 100);

        // Create consumer group and consume messages
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.seekToEnd();
        consumer.commit();
        consumer.close();

        // Verify offsets are not at earliest
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), groupId);
        assertTrue(offsetsBefore.values().stream().anyMatch(o -> o.offset() > 0));

        // Alter offsets to earliest
        LaunchResult result = launcher.launch("group", "alter", groupId,
                "--to-earliest", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));
        assertTrue(result.getOutput().contains(groupId));

        // Verify offsets were reset to 0
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), groupId);
        for (OffsetAndMetadata offset : offsetsAfter.values()) {
            assertEquals(0, offset.offset());
        }
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - alter not supported for STREAMS groups
    })
    void testAlterGroupToLatest(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "alter-latest-topic-" + groupType.name().toLowerCase();
        String groupId = "alter-latest-group-" + groupType.name().toLowerCase();

        // Create topic and produce messages
        topicService.createTopic(admin(), topicName, 1, 1, Collections.emptyMap());
        produceMessages(topicName, 100);

        // Create consumer group with offsets at 0
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.commit();
        consumer.close();

        // Alter offsets to latest
        LaunchResult result = launcher.launch("group", "alter", groupId,
                "--to-latest", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify offsets are at end (50 messages / 2 partitions = ~25 per partition)
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), groupId);
        for (OffsetAndMetadata offset : offsetsAfter.values()) {
            assertTrue(offset.offset() >= 20, () -> "Offset was " + offset.offset());  // Should be around 25
        }
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - alter not supported for STREAMS groups
    })
    void testAlterGroupToSpecificOffset(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "alter-offset-topic-" + groupType.name().toLowerCase();
        String groupId = "alter-offset-group-" + groupType.name().toLowerCase();

        // Create topic
        topicService.createTopic(admin(), topicName, 2, 1, Collections.emptyMap());
        produceMessages(topicName, 100);

        // Create consumer group
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.seekToBeginning();
        consumer.commit();
        consumer.close();

        // Alter specific partition to offset 10
        LaunchResult result = launcher.launch("group", "alter", groupId,
                "--to-offset", "10=" + topicName + ":0", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify only partition 0 was changed to offset 10
        var offsets = groupService.getGroupOffsetMap(admin(), groupId);
        OffsetAndMetadata partition0 = offsets.get(new TopicPartition(topicName, 0));
        assertNotNull(partition0);
        assertEquals(10, partition0.offset());
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - alter not supported for STREAMS groups
    })
    void testAlterGroupShiftBy(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "shift-topic-" + groupType.name().toLowerCase();
        String groupId = "shift-group-" + groupType.name().toLowerCase();

        // Create topic with 3 partitions
        topicService.createTopic(admin(), topicName, 3, 1, Collections.emptyMap());
        produceMessages(topicName, 100);

        // Create consumer group
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.commit();
        consumer.close();

        // Get current offsets
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), groupId);

        // Shift partition 0 forward by 5, partition 2 back by 3, leave partition 1 unchanged
        LaunchResult result = launcher.launch("group", "alter", groupId,
                "--shift-by", "5=" + topicName + ":0",
                "--shift-by", "-3=" + topicName + ":2",
                "--yes");

        assertEquals(0, result.exitCode());

        // Verify only specified partitions were shifted
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), groupId);
        TopicPartition partition0 = new TopicPartition(topicName, 0);
        TopicPartition partition1 = new TopicPartition(topicName, 1);
        TopicPartition partition2 = new TopicPartition(topicName, 2);

        long before0 = offsetsBefore.get(partition0).offset();
        long after0 = offsetsAfter.get(partition0).offset();
        assertEquals(before0 + 5, after0, "Partition 0 should be shifted forward by 5");

        long before1 = offsetsBefore.get(partition1).offset();
        long after1 = offsetsAfter.get(partition1).offset();
        assertEquals(before1, after1, "Partition 1 should remain unchanged");

        long before2 = offsetsBefore.get(partition2).offset();
        long after2 = offsetsAfter.get(partition2).offset();
        assertEquals(before2 - 3, after2, "Partition 2 should be shifted back by 3");
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - alter not supported for STREAMS groups
    })
    void testAlterGroupToDatetime(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "datetime-topic-" + groupType.name().toLowerCase();
        String groupId = "datetime-group-" + groupType.name().toLowerCase();

        // Create topic
        topicService.createTopic(admin(), topicName, 1, 1, Collections.emptyMap());

        // Produce messages with specific timestamps
        var baseTimestamp = Instant.now().minus(Duration.ofHours(1)); // 1 hour ago
        produceMessagesWithTimestamps(topicName, 50, baseTimestamp.toEpochMilli(), 60000); // 1 message per minute

        // Create consumer group
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.commit();
        // Close so that the group may be altered
        consumer.close();

        // Reset to timestamp 30 minutes ago (should be around message 30)
        long targetTimestamp = baseTimestamp.plus(Duration.ofMinutes(30)).toEpochMilli(); // 30 minutes after base
        String isoTimestamp = Instant.ofEpochMilli(targetTimestamp).toString();

        LaunchResult result = launcher.launch("group", "alter", groupId,
                "--to-datetime", isoTimestamp, "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify offsets were set to approximately the target timestamp
        var offsets = groupService.getGroupOffsetMap(admin(), groupId);
        var offsetValue = offsets.values().iterator().next().offset();
        assertEquals(30, offsetValue);
    }

    @ParameterizedTest
    @CsvSource({
        "30, PT30M, 60",
        "90, PT-1800S, 60",
        "119, PT-1H, 59", // 119 - 60 = 59
        "120, PT-1H, 61", // 120 is "next" offset, so the calculation uses "now" and "now" - 60 > 60, so 61 is next
    })
    void testAlterGroupByDuration(long startOffset, String duration, int expectedOffset) throws Exception {
        // Create topic
        topicService.createTopic(admin(), "duration-topic", 1, 1, Collections.emptyMap());

        // Produce messages with timestamps spread over 2 hours
        var baseTimestamp = Instant.now().minus(Duration.ofHours(2)); // 2 hours ago
        produceMessagesWithTimestamps("duration-topic", 120, baseTimestamp.toEpochMilli(), 60000); // 1 message per minute

        var consumer = createConsumer(GroupType.CONSUMER, GroupProtocol.CLASSIC, "duration-group", "duration-topic").join();
        consumer.seek(Map.of(new TopicPartition("duration-topic", 0), startOffset));
        consumer.commit();
        consumer.close();

        LaunchResult result = launcher.launch("group", "alter", "duration-group",
                "--by-duration", duration, "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify offset was shifted
        var offsets = groupService.getGroupOffsetMap(admin(), "duration-group");
        var offsetValue = offsets.values().iterator().next().offset();
        assertEquals(expectedOffset, offsetValue);
    }

    @Test
    void testAlterGroupByDurationOffsetNotFound() throws Exception {
        // Create topic
        topicService.createTopic(admin(), "duration-topic", 1, 1, Collections.emptyMap());

        // Produce messages with timestamps spread over 2 hours
        var baseTimestamp = Instant.now().minus(Duration.ofHours(2)); // 2 hours ago
        produceMessagesWithTimestamps("duration-topic", 120, baseTimestamp.toEpochMilli(), 60000); // 1 message per minute

        var consumer = createConsumer(GroupType.CONSUMER, GroupProtocol.CLASSIC, "duration-group", "duration-topic")
                .join();
        consumer.seek(Map.of(new TopicPartition("duration-topic", 0), 120L));
        consumer.commit();
        consumer.close();

        LaunchResult result = launcher.launch("group", "alter", "duration-group",
                "--by-duration", "PT1H", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Warning: no offset found when adjusting timestamp"));

        // Verify offset was unchanged
        var offsets = groupService.getGroupOffsetMap(admin(), "duration-group");
        var offsetValue = offsets.values().iterator().next().offset();
        assertEquals(120, offsetValue);
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        // "STREAMS ,         ,", // Kafka 4.1 limitation - alter not supported for STREAMS groups
    })
    void testAlterGroupDeleteOffsets(GroupType groupType, GroupProtocol groupProtocol) throws Exception {
        String topicName = "delete-offset-topic-" + groupType.name().toLowerCase();
        String groupId = "delete-offset-group-" + groupType.name().toLowerCase();

        // Create topic
        topicService.createTopic(admin(), topicName, 2, 1, Collections.emptyMap());
        produceMessages(topicName, 50);

        // Create consumer group
        var consumer = createConsumer(groupType, groupProtocol, groupId, topicName).join();
        consumer.commit();
        consumer.close();

        // Verify offsets exist
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), groupId);
        assertEquals(2, offsetsBefore.size());

        // Delete offsets for partition 0
        LaunchResult result = launcher.launch("group", "alter", groupId,
                "--delete", topicName + ":0", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Deleted offsets"));

        // Verify partition 0 offset was deleted
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), groupId);
        assertEquals(1, offsetsAfter.size());
        assertFalse(offsetsAfter.containsKey(new TopicPartition(topicName, 0)));
        assertTrue(offsetsAfter.containsKey(new TopicPartition(topicName, 1)));
    }

    @Test
    void testAlterGroupWithTopicOnly() throws Exception {
        // Create two topics
        topicService.createTopic(admin(), "topic-a", 2, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "topic-b", 2, 1, Collections.emptyMap());
        produceMessages("topic-a", 50);
        produceMessages("topic-b", 50);

        // Create consumer group consuming from both topics
        var consumer = createClassicConsumer("multi-topic-group", "topic-a", "topic-b").join();
        consumer.poll(Duration.ofSeconds(3));
        consumer.commit();
        consumer.close();

        // Reset only topic-a to earliest
        LaunchResult result = launcher.launch("group", "alter", "multi-topic-group",
                "--to-earliest", "topic-a", "--yes");

        assertEquals(0, result.exitCode());

        // Verify only topic-a partitions were reset
        var offsets = groupService.getGroupOffsetMap(admin(), "multi-topic-group");
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getKey().topic().equals("topic-a")) {
                assertEquals(0, entry.getValue().offset());
            } else if (entry.getKey().topic().equals("topic-b")) {
                assertTrue(entry.getValue().offset() > 0);
            }
        }
    }

    @Test
    void testAlterGroupActiveMembers() throws Exception {
        // Create topic
        topicService.createTopic(admin(), "active-topic", 2, 1, Collections.emptyMap());

        // Create consumer group with active consumer (don't close it)
        var consumer = createClassicConsumer("active-group", "active-topic").join();

        // Try to alter offsets - should fail
        LaunchResult result = launcher.launch("group", "alter", "active-group",
                "--to-earliest", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("has active members"));
        assertTrue(result.getErrorOutput().contains("Stop all consumers"));

        // Clean up
        consumer.close();
    }

    @Test
    void testAlterGroupNotFound() {
        LaunchResult result = launcher.launch("group", "alter", "nonexistent-group",
                "--to-earliest", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("not found"));
    }

    @Test
    void testAlterGroupNoOffsets() throws Exception {
        // Create topic and group
        topicService.createTopic(admin(), "no-offsets-topic", 1, 1, Collections.emptyMap());
        var consumer = createClassicConsumer("no-offsets-group", "no-offsets-topic").join();
        consumer.close();
        groupService.deleteGroupOffsets(admin(), "no-offsets-group", Set.of(new TopicPartition("no-offsets-topic", 0)));

        LaunchResult result = launcher.launch("group", "alter", "no-offsets-group",
                "--to-earliest", "--yes");

        assertEquals(1, result.exitCode());
        // Group with no offsets returns "has no committed offsets" error
        assertTrue(result.getErrorOutput().contains("has no committed offsets"));
    }

    @Test
    void testAlterGroupNoOptions() throws Exception {
        // Create topic and group
        topicService.createTopic(admin(), "no-opts-topic", 1, 1, Collections.emptyMap());
        var consumer = createClassicConsumer("no-opts-group", "no-opts-topic").join();
        consumer.poll(Duration.ofMillis(100));
        consumer.commit();
        consumer.close();

        // Try to alter without any options
        LaunchResult result = launcher.launch("group", "alter", "no-opts-group");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("At least one offset option must be specified"));
    }

    @Test
    void testAlterGroupNoContext() {
        // Delete the context
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("group", "alter", "some-group",
                "--to-earliest", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }
}
