package io.streamshub.clik.command.group;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
        createConsumer("consumer-group-1", GroupProtocol.CLASSIC, "filter-topic").join();

        // Filter by consumer type
        LaunchResult consumerResult = launcher.launch("group", "list", "--type", "classic");
        assertEquals(0, consumerResult.exitCode());
        assertTrue(consumerResult.getOutput().contains("consumer-group-1"));

        // Filter by share type (should be empty)
        LaunchResult shareResult = launcher.launch("group", "list", "--type", "share");
        assertEquals(0, shareResult.exitCode());
        assertTrue(shareResult.getOutput().contains("No groups found"));
    }

    @Test
    void testDescribeGroup() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "describe-topic", 3, 1, Collections.emptyMap());
        createClassicConsumer("describe-group", "describe-topic").join();

        LaunchResult result = launcher.launch("group", "describe", "describe-group");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Group: describe-group"));
        assertTrue(output.contains("Type:"));
        assertTrue(output.contains("State:"));
    }

    @Test
    void testDescribeGroupWithMembers() throws Exception {
        // Create test topic and multiple consumers
        topicService.createTopic(admin(), "members-topic", 6, 1, Collections.emptyMap());

        CompletableFuture.allOf(
                createClassicConsumer("multi-member-group", "members-topic"),
                createClassicConsumer("multi-member-group", "members-topic"),
                createClassicConsumer("multi-member-group", "members-topic")
        ).join();

        LaunchResult result = launcher.launch("group", "describe", "multi-member-group");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Group: multi-member-group"));
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

    @Test
    void testDescribeGroupJsonFormat() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "json-describe-topic", 2, 1, Collections.emptyMap());
        createClassicConsumer("json-describe-group", "json-describe-topic").join();

        LaunchResult result = launcher.launch("group", "describe", "json-describe-group", "-o", "json");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("\"groupId\""));
        assertTrue(output.contains("\"json-describe-group\""));
        assertTrue(output.contains("\"type\""));
        assertTrue(output.contains("\"state\""));
    }

    @Test
    void testDescribeGroupYamlFormat() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "yaml-describe-topic", 2, 1, Collections.emptyMap());
        createClassicConsumer("yaml-describe-group", "yaml-describe-topic").join();

        LaunchResult result = launcher.launch("group", "describe", "yaml-describe-group", "-o", "yaml");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("groupId:"));
        assertTrue(output.contains("yaml-describe-group"));
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

    @Test
    void testDescribeGroupWithOffsets() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "offset-test-topic", 2, 1, Collections.emptyMap());

        // Produce some messages
        produceMessages("offset-test-topic", 50);

        // Create consumer and consume messages
        Consumer<String, String> consumer = createClassicConsumer("offset-test-group", "offset-test-topic")
                .join();

        consumer.commitSync();

        LaunchResult result = launcher.launch("group", "describe", "offset-test-group");
        assertEquals(0, result.exitCode());
        String output = result.getOutput();
        assertTrue(output.contains("Group: offset-test-group"));
        assertTrue(output.contains("Topic Lag:"));
        assertTrue(output.contains("TOPIC"));
        assertTrue(output.contains("PARTITION"));
        assertTrue(output.contains("CURRENT OFFSET"));
        assertTrue(output.contains("LOG END OFFSET"));
        assertTrue(output.contains("LAG"));
    }

    @Test
    void testDeleteGroup() throws Exception {
        // Create test topic and consumer group
        topicService.createTopic(admin(), "delete-test-topic", 2, 1, Collections.emptyMap());
        Consumer<String, String> consumer = createClassicConsumer("delete-test-group", "delete-test-topic").join();

        // Close the consumer so the group becomes empty/eligible for deletion
        close(consumer);

        LaunchResult result = launcher.launch("group", "delete", "delete-test-group", "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Group \"delete-test-group\" deleted"));

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
        var consumer1 = createConsumer("delete1", GroupProtocol.CLASSIC, "multi-delete-topic");
        var consumer2 = createConsumer("delete2", GroupProtocol.CONSUMER, "multi-delete-topic");
        var consumer3 = createShareConsumer("delete3", "multi-delete-topic");
        CompletableFuture.allOf(consumer1, consumer2, consumer3).join();

        // Close all consumers so the groups become empty/eligible for deletion
        close(consumer1.join());
        close(consumer2.join());
        close(consumer3.join());

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

    @Test
    void testAlterGroupToEarliest() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "alter-earliest-topic", 3, 1, Collections.emptyMap());
        produceMessages("alter-earliest-topic", 100);

        // Create consumer group and consume messages
        Consumer<String, String> consumer = createClassicConsumer("alter-earliest-group", "alter-earliest-topic").join();
        var assignment = consumer.assignment();
        consumer.seekToEnd(assignment);
        assignment.forEach(consumer::position);
        consumer.commitSync();
        close(consumer);

        // Verify offsets are not at earliest
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), "alter-earliest-group");
        assertTrue(offsetsBefore.values().stream().anyMatch(o -> o.offset() > 0));

        // Alter offsets to earliest
        LaunchResult result = launcher.launch("group", "alter", "alter-earliest-group",
                "--to-earliest", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));
        assertTrue(result.getOutput().contains("alter-earliest-group"));

        // Verify offsets were reset to 0
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), "alter-earliest-group");
        for (OffsetAndMetadata offset : offsetsAfter.values()) {
            assertEquals(0, offset.offset());
        }
    }

    @Test
    void testAlterGroupToLatest() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "alter-latest-topic", 2, 1, Collections.emptyMap());
        produceMessages("alter-latest-topic", 50);

        // Create consumer group with offsets at 0
        Consumer<String, String> consumer = createClassicConsumer("alter-latest-group", "alter-latest-topic").join();
        var assignment = consumer.assignment();
        consumer.seekToBeginning(assignment);
        assignment.forEach(consumer::position);
        consumer.commitSync();
        close(consumer);

        // Alter offsets to latest
        LaunchResult result = launcher.launch("group", "alter", "alter-latest-group",
                "--to-latest", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify offsets are at end (50 messages / 2 partitions = ~25 per partition)
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), "alter-latest-group");
        for (OffsetAndMetadata offset : offsetsAfter.values()) {
            assertTrue(offset.offset() >= 20);  // Should be around 25
        }
    }

    @Test
    void testAlterGroupToSpecificOffset() throws Exception {
        // Create topic
        topicService.createTopic(admin(), "alter-offset-topic", 2, 1, Collections.emptyMap());
        produceMessages("alter-offset-topic", 100);

        // Create consumer group
        Consumer<String, String> consumer = createClassicConsumer("alter-offset-group", "alter-offset-topic").join();
        consumer.commitSync();
        close(consumer);

        // Alter specific partition to offset 10
        LaunchResult result = launcher.launch("group", "alter", "alter-offset-group",
                "--to-offset", "10=alter-offset-topic:0", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify only partition 0 was changed to offset 10
        var offsets = groupService.getGroupOffsetMap(admin(), "alter-offset-group");
        OffsetAndMetadata partition0 = offsets.get(new TopicPartition("alter-offset-topic", 0));
        assertNotNull(partition0);
        assertEquals(10, partition0.offset());
    }

    @Test
    void testAlterGroupShiftBy() throws Exception {
        // Create topic with 3 partitions
        topicService.createTopic(admin(), "shift-topic", 3, 1, Collections.emptyMap());
        produceMessages("shift-topic", 100);

        // Create consumer group
        Consumer<String, String> consumer = createClassicConsumer("shift-group", "shift-topic").join();
        consumer.commitSync();
        close(consumer);

        // Get current offsets
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), "shift-group");

        // Shift partition 0 forward by 5, partition 2 back by 3, leave partition 1 unchanged
        LaunchResult result = launcher.launch("group", "alter", "shift-group",
                "--shift-by", "5=shift-topic:0",
                "--shift-by", "-3=shift-topic:2",
                "--yes");

        assertEquals(0, result.exitCode());

        // Verify only specified partitions were shifted
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), "shift-group");
        TopicPartition partition0 = new TopicPartition("shift-topic", 0);
        TopicPartition partition1 = new TopicPartition("shift-topic", 1);
        TopicPartition partition2 = new TopicPartition("shift-topic", 2);

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

    @Test
    void testAlterGroupToDatetime() throws Exception {
        // Create topic
        topicService.createTopic(admin(), "datetime-topic", 1, 1, Collections.emptyMap());

        // Produce messages with specific timestamps
        var baseTimestamp = Instant.now().minus(Duration.ofHours(1)); // 1 hour ago
        produceMessagesWithTimestamps("datetime-topic", 50, baseTimestamp.toEpochMilli(), 60000); // 1 message per minute

        // Create consumer group
        var consumer = createClassicConsumer("datetime-group", "datetime-topic").join();
        consumer.commitSync();
        // Close so that the group may be altered
        close(consumer);

        // Reset to timestamp 30 minutes ago (should be around message 30)
        long targetTimestamp = baseTimestamp.plus(Duration.ofMinutes(30)).toEpochMilli(); // 30 minutes after base
        String isoTimestamp = Instant.ofEpochMilli(targetTimestamp).toString();

        LaunchResult result = launcher.launch("group", "alter", "datetime-group",
                "--to-datetime", isoTimestamp, "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Altered offsets"));

        // Verify offsets were set to approximately the target timestamp
        var offsets = groupService.getGroupOffsetMap(admin(), "datetime-group");
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
    void testAlterGroupByDuration(int startOffset, String duration, int expectedOffset) throws Exception {
        // Create topic
        topicService.createTopic(admin(), "duration-topic", 1, 1, Collections.emptyMap());

        // Produce messages with timestamps spread over 2 hours
        var baseTimestamp = Instant.now().minus(Duration.ofHours(2)); // 2 hours ago
        produceMessagesWithTimestamps("duration-topic", 120, baseTimestamp.toEpochMilli(), 60000); // 1 message per minute

        var consumer = createClassicConsumer("duration-group", "duration-topic").join();
        for (var partition : consumer.assignment()) {
            consumer.seek(partition, startOffset);
        }
        consumer.commitSync();
        close(consumer);

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

        var consumer = createClassicConsumer("duration-group", "duration-topic").join();
        for (var partition : consumer.assignment()) {
            consumer.seek(partition, 120);
        }
        consumer.commitSync();
        close(consumer);

        LaunchResult result = launcher.launch("group", "alter", "duration-group",
                "--by-duration", "PT1H", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Warning: no offset found when adjusting timestamp"));

        // Verify offset was unchanged
        var offsets = groupService.getGroupOffsetMap(admin(), "duration-group");
        var offsetValue = offsets.values().iterator().next().offset();
        assertEquals(120, offsetValue);
    }

    @Test
    void testAlterGroupDeleteOffsets() throws Exception {
        // Create topic
        topicService.createTopic(admin(), "delete-offset-topic", 2, 1, Collections.emptyMap());
        produceMessages("delete-offset-topic", 50);

        // Create consumer group
        Consumer<String, String> consumer = createClassicConsumer("delete-offset-group", "delete-offset-topic").join();
        consumer.commitSync();
        close(consumer);

        // Verify offsets exist
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), "delete-offset-group");
        assertEquals(2, offsetsBefore.size());

        // Delete offsets for partition 0
        LaunchResult result = launcher.launch("group", "alter", "delete-offset-group",
                "--delete", "delete-offset-topic:0", "--yes");

        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Deleted offsets"));

        // Verify partition 0 offset was deleted
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), "delete-offset-group");
        assertEquals(1, offsetsAfter.size());
        assertFalse(offsetsAfter.containsKey(new TopicPartition("delete-offset-topic", 0)));
        assertTrue(offsetsAfter.containsKey(new TopicPartition("delete-offset-topic", 1)));
    }

    @Test
    void testAlterGroupWithTopicOnly() throws Exception {
        // Create two topics
        topicService.createTopic(admin(), "topic-a", 2, 1, Collections.emptyMap());
        topicService.createTopic(admin(), "topic-b", 2, 1, Collections.emptyMap());
        produceMessages("topic-a", 50);
        produceMessages("topic-b", 50);

        // Create consumer group consuming from both topics
        Consumer<String, String> consumer = createClassicConsumer("multi-topic-group", "topic-a").join();
        consumer.subscribe(List.of("topic-a", "topic-b"));
        consumer.poll(Duration.ofSeconds(3));
        consumer.commitSync();
        close(consumer);

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
        Consumer<String, String> consumer = createClassicConsumer("active-group", "active-topic").join();

        // Try to alter offsets - should fail
        LaunchResult result = launcher.launch("group", "alter", "active-group",
                "--to-earliest", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("has active members"));
        assertTrue(result.getErrorOutput().contains("Stop all consumers"));

        // Clean up
        close(consumer);
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
        Consumer<String, String> consumer = createClassicConsumer("no-offsets-group", "no-offsets-topic").join();
        close(consumer);
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
        Consumer<String, String> consumer = createClassicConsumer("no-opts-group", "no-opts-topic").join();
        consumer.poll(Duration.ofMillis(100));
        consumer.commitSync();
        close(consumer);

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
