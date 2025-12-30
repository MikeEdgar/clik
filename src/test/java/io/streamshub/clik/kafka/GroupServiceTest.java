package io.streamshub.clik.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.kafka.model.GroupInfo;
import io.streamshub.clik.kafka.model.OffsetLagInfo;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class GroupServiceTest extends ClikTestBase {

    @Inject
    Logger logger;

    @Inject
    GroupService groupService;

    @Inject
    TopicService topicService;

    @Test
    void testListGroupsEmpty() throws Exception {
        // Note: Kafka may have internal consumer groups (like __consumer_offsets consumer)
        // So we just verify the list operation works, not that it's strictly empty
        Collection<GroupInfo> groups = groupService.listGroups(admin(), null);
        assertNotNull(groups);
    }

    @Test
    void testListGroupsConsumer() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "test-topic", 3, 1, null);

        // Create consumer groups
        CompletableFuture.allOf(
                createConsumerGroup("test-group-1", "test-topic"),
                createConsumerGroup("test-group-2", "test-topic")
        ).join();

        Collection<GroupInfo> groups = groupService.listGroups(admin(), null);
        assertTrue(groups.size() >= 2, "Should have at least 2 groups");

        // Verify our test group IDs are present
        List<String> groupIds = groups.stream()
                .map(GroupInfo::getGroupId)
                .sorted()
                .toList();
        assertTrue(groupIds.contains("test-group-1"));
        assertTrue(groupIds.contains("test-group-2"));

        // Verify basic metadata is present
        for (GroupInfo group : groups) {
            assertNotNull(group.getGroupId());
            assertNotNull(group.getType());
            assertNotNull(group.getState());
            assertTrue(group.getMemberCount() >= 0);
            // Members and offsets should be null for list operation
            assertNull(group.getMembers());
            assertNull(group.getOffsets());
        }
    }

    @Test
    void testListGroupsFilterByType() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "test-topic", 3, 1, null);

        // Create consumer group
        createConsumerGroup("consumer-group", "test-topic", "consumer").join();

        // Filter by consumer type
        Collection<GroupInfo> consumerGroups = groupService.listGroups(admin(), "consumer");
        assertTrue(consumerGroups.size() >= 1, "Should have at least 1 consumer group");

        // Verify our test group is present
        boolean foundTestGroup = consumerGroups.stream()
                .anyMatch(g -> "consumer-group".equals(g.getGroupId()));
        assertTrue(foundTestGroup, "Should find consumer-group");

        // Filter by share type (should be empty)
        Collection<GroupInfo> shareGroups = groupService.listGroups(admin(), "share");
        assertTrue(shareGroups.isEmpty());

        // Filter by stream type (should be empty)
        Collection<GroupInfo> streamGroups = groupService.listGroups(admin(), "stream");
        assertTrue(streamGroups.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = { "consumer", "classic" })
    void testDescribeConsumerGroup(String groupProtocol) throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "describe-topic", 3, 1, null);

        // Create consumer group
        createConsumerGroup("describe-group", "describe-topic", groupProtocol).join();

        GroupInfo group = groupService.describeGroup(admin(), "describe-group");
        assertNotNull(group);
        assertEquals("describe-group", group.getGroupId());
        assertEquals(groupProtocol, group.getType());
        assertNotNull(group.getState());
        assertEquals(1, group.getMemberCount());
        assertNotNull(group.getCoordinator());
        assertNotNull(group.getMembers());
        assertEquals(1, group.getMembers().size());
    }

    @Test
    void testDescribeGroupWithMembers() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "members-topic", 6, 1, null);

        // Produce some messages
        produceMessages("members-topic", 100);

        // Create multiple consumers in the same group
        CompletableFuture.allOf(
                createConsumerGroup("multi-member-group", "members-topic"),
                createConsumerGroup("multi-member-group", "members-topic"),
                createConsumerGroup("multi-member-group", "members-topic")
        ).join();

        GroupInfo group = groupService.describeGroup(admin(), "multi-member-group");
        assertNotNull(group);
        assertEquals("multi-member-group", group.getGroupId());
        assertEquals(3, group.getMemberCount());
        assertNotNull(group.getMembers());
        assertEquals(3, group.getMembers().size());

        // Verify member info
        for (var member : group.getMembers()) {
            assertNotNull(member.getMemberId());
            assertNotNull(member.getClientId());
            assertNotNull(member.getHost());
            assertNotNull(member.getAssignments());
        }
    }

    @Test
    void testDescribeGroupNotFound() throws Exception {
        GroupInfo group = groupService.describeGroup(admin(), "non-existent-group");
        assertNull(group);
    }

    @Test
    void testDescribeGroupOffsets() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "offset-topic", 2, 1, null);

        // Produce some messages
        produceMessages("offset-topic", 100);

        // Create consumer and consume some messages
        Consumer<String, String> consumer = createConsumerGroup("offset-group", "offset-topic")
                .join();

        // Poll and commit offsets
        consumer.poll(Duration.ofSeconds(5));
        consumer.commitSync();

        GroupInfo group = groupService.describeGroup(admin(), "offset-group");
        assertNotNull(group);
        assertEquals("offset-group", group.getGroupId());

        // Verify offsets are present
        List<OffsetLagInfo> offsets = group.getOffsets();
        assertNotNull(offsets);
        assertFalse(offsets.isEmpty());

        // Verify offset information
        for (OffsetLagInfo offset : offsets) {
            assertEquals("offset-topic", offset.getTopic());
            assertTrue(offset.getPartition() >= 0);
            assertNotNull(offset.getCurrentOffset());
            assertNotNull(offset.getLogEndOffset());
            assertNotNull(offset.getLag());
            assertTrue(offset.getLag() >= 0);
        }
    }

    @Test
    void testDescribeGroupNoOffsets() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "no-offset-topic", 2, 1, null);

        // Create consumer but don't poll or commit
        createConsumerGroup("no-offset-group", "no-offset-topic").join();

        GroupInfo group = groupService.describeGroup(admin(), "no-offset-group");
        assertNotNull(group);
        assertEquals("no-offset-group", group.getGroupId());

        // Offsets may be empty or null for a group with no committed offsets
        List<OffsetLagInfo> offsets = group.getOffsets();
        if (offsets != null) {
            // If offsets are present, they should be empty or have null current offsets
            assertTrue(offsets.isEmpty() || offsets.get(0).getCurrentOffset() == null);
        }
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
