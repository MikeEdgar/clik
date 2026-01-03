package io.streamshub.clik.command.group;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class GroupCommandTest extends ClikMainTestBase {

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
                createConsumerGroup("test-group-1", "group-test-topic"),
                createConsumerGroup("test-group-2", "group-test-topic")
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
                createConsumerGroup("alpha-group", "name-test-topic"),
                createConsumerGroup("beta-group", "name-test-topic"),
                createConsumerGroup("gamma-group", "name-test-topic")
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
        createConsumerGroup("json-group", "json-group-topic").join();

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
        createConsumerGroup("yaml-group", "yaml-group-topic").join();

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
        createConsumerGroup("consumer-group-1", "filter-topic", "classic").join();

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
        createConsumerGroup("describe-group", "describe-topic").join();

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
                createConsumerGroup("multi-member-group", "members-topic"),
                createConsumerGroup("multi-member-group", "members-topic"),
                createConsumerGroup("multi-member-group", "members-topic")
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
        createConsumerGroup("json-describe-group", "json-describe-topic").join();

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
        createConsumerGroup("yaml-describe-group", "yaml-describe-topic").join();

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
        Consumer<String, String> consumer = createConsumerGroup("offset-test-group", "offset-test-topic")
                .join();

        // Poll and commit offsets
        consumer.poll(Duration.ofSeconds(5));
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
        Consumer<String, String> consumer = createConsumerGroup("delete-test-group", "delete-test-topic").join();

        // Close the consumer so the group becomes empty/eligible for deletion
        close(consumer);

        LaunchResult result = launcher.launch("group", "delete", "delete-test-group", "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("Group \"delete-test-group\" deleted"));

        // Verify group was deleted
        var groups = groupService.listGroups(admin(), null);
        assertFalse(groups.stream().anyMatch(g -> g.getGroupId().equals("delete-test-group")));
    }

    @Test
    void testDeleteMultipleGroups() throws Exception {
        // Create test topic and consumer groups
        topicService.createTopic(admin(), "multi-delete-topic", 2, 1, Collections.emptyMap());
        var consumer1 = createConsumerGroup("delete1", "multi-delete-topic");
        var consumer2 = createConsumerGroup("delete2", "multi-delete-topic");
        var consumer3 = createConsumerGroup("delete3", "multi-delete-topic");
        CompletableFuture.allOf(consumer1, consumer2, consumer3).join();

        // Close all consumers so the groups become empty/eligible for deletion
        close(consumer1.join());
        close(consumer2.join());
        close(consumer3.join());

        LaunchResult result = launcher.launch("group", "delete", "delete1", "delete2", "delete3", "--yes");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("3 groups deleted"));

        // Verify groups were deleted
        var groups = groupService.listGroups(admin(), null);
        assertFalse(groups.stream().anyMatch(g -> g.getGroupId().equals("delete1")));
        assertFalse(groups.stream().anyMatch(g -> g.getGroupId().equals("delete2")));
        assertFalse(groups.stream().anyMatch(g -> g.getGroupId().equals("delete3")));
    }

    @Test
    void testDeleteGroupNotFound() {
        LaunchResult result = launcher.launch("group", "delete", "nonexistent-group", "--yes");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Failed to delete group(s)"));
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
        Consumer<String, String> consumer = createConsumerGroup("alter-earliest-group", "alter-earliest-topic").join();
        consumer.poll(Duration.ofSeconds(5));
        consumer.commitSync();
        close(consumer);

        // Verify offsets are not at earliest
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), "alter-earliest-group");
        assertTrue(offsetsBefore.values().stream().anyMatch(o -> o.offset() > 0));

        // Alter offsets to earliest
        LaunchResult result = launcher.launch("group", "alter", "alter-earliest-group",
                "--to-earliest", "", "--yes");

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
        Consumer<String, String> consumer = createConsumerGroup("alter-latest-group", "alter-latest-topic").join();
        consumer.poll(Duration.ofMillis(100)); // Don't consume messages
        consumer.commitSync();
        close(consumer);

        // Alter offsets to latest
        LaunchResult result = launcher.launch("group", "alter", "alter-latest-group",
                "--to-latest", "", "--yes");

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
        Consumer<String, String> consumer = createConsumerGroup("alter-offset-group", "alter-offset-topic").join();
        consumer.poll(Duration.ofMillis(100));
        consumer.commitSync();
        close(consumer);

        // Alter specific partition to offset 10
        LaunchResult result = launcher.launch("group", "alter", "alter-offset-group",
                "--to-offset", "10:alter-offset-topic:0", "--yes");

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
        // Create topic
        topicService.createTopic(admin(), "shift-topic", 2, 1, Collections.emptyMap());
        produceMessages("shift-topic", 100);

        // Create consumer group at offset 20
        Consumer<String, String> consumer = createConsumerGroup("shift-group", "shift-topic").join();
        consumer.poll(Duration.ofSeconds(2));
        consumer.commitSync();
        close(consumer);

        // Get current offsets
        var offsetsBefore = groupService.getGroupOffsetMap(admin(), "shift-group");

        // Shift forward by 5
        LaunchResult result = launcher.launch("group", "alter", "shift-group",
                "--shift-by", "5:", "--yes");

        assertEquals(0, result.exitCode());

        // Verify offsets increased by 5
        var offsetsAfter = groupService.getGroupOffsetMap(admin(), "shift-group");
        for (TopicPartition tp : offsetsBefore.keySet()) {
            long before = offsetsBefore.get(tp).offset();
            long after = offsetsAfter.get(tp).offset();
            assertEquals(before + 5, after);
        }
    }

    @Test
    void testAlterGroupDeleteOffsets() throws Exception {
        // Create topic
        topicService.createTopic(admin(), "delete-offset-topic", 2, 1, Collections.emptyMap());
        produceMessages("delete-offset-topic", 50);

        // Create consumer group
        Consumer<String, String> consumer = createConsumerGroup("delete-offset-group", "delete-offset-topic").join();
        consumer.poll(Duration.ofSeconds(2));
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
        Consumer<String, String> consumer = createConsumerGroup("multi-topic-group", "topic-a").join();
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
        Consumer<String, String> consumer = createConsumerGroup("active-group", "active-topic").join();

        // Try to alter offsets - should fail
        LaunchResult result = launcher.launch("group", "alter", "active-group",
                "--to-earliest", "", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("has active members"));
        assertTrue(result.getErrorOutput().contains("Stop all consumers"));

        // Clean up
        close(consumer);
    }

    @Test
    void testAlterGroupNotFound() {
        LaunchResult result = launcher.launch("group", "alter", "nonexistent-group",
                "--to-earliest", "", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("not found"));
    }

    @Test
    void testAlterGroupNoOffsets() throws Exception {
        // Create topic and group
        topicService.createTopic(admin(), "no-offsets-topic", 1, 1, Collections.emptyMap());
        Consumer<String, String> consumer = createConsumerGroup("no-offsets-group", "no-offsets-topic").join();
        close(consumer);
        groupService.deleteGroupOffsets(admin(), "no-offsets-group", Set.of(new TopicPartition("no-offsets-topic", 0)));

        LaunchResult result = launcher.launch("group", "alter", "no-offsets-group",
                "--to-earliest", "", "--yes");

        assertEquals(1, result.exitCode());
        // Group with no offsets returns "has no committed offsets" error
        assertTrue(result.getErrorOutput().contains("has no committed offsets"));
    }

    @Test
    void testAlterGroupNoOptions() throws Exception {
        // Create topic and group
        topicService.createTopic(admin(), "no-opts-topic", 1, 1, Collections.emptyMap());
        Consumer<String, String> consumer = createConsumerGroup("no-opts-group", "no-opts-topic").join();
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
                "--to-earliest", "", "--yes");

        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    /**
     * Helper method to produce test messages
     */
    private void produceMessages(String topic, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)).get();
            }
        }
    }
}
