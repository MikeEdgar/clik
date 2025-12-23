package io.streamshub.clik.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.quarkus.test.junit.QuarkusTest;
import io.streamshub.clik.kafka.model.GroupInfo;
import io.streamshub.clik.kafka.model.OffsetLagInfo;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class GroupServiceTest {

    @Inject
    Logger logger;

    @Inject
    GroupService groupService;

    @Inject
    TopicService topicService;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    Admin admin;
    List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBootstrapServers);
        admin = Admin.create(props);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close all consumers
        for (KafkaConsumer<String, String> consumer : consumers) {
            try {
                consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        consumers.clear();

        // Clean up all test topics
        Set<String> topics = topicService.listTopics(admin, false);
        if (!topics.isEmpty()) {
            topicService.deleteTopics(admin, topics);
        }

        if (admin != null) {
            admin.close();
        }
    }

    @Test
    void testListGroupsEmpty() throws Exception {
        // Note: Kafka may have internal consumer groups (like __consumer_offsets consumer)
        // So we just verify the list operation works, not that it's strictly empty
        Collection<GroupInfo> groups = groupService.listGroups(admin, null);
        assertNotNull(groups);
    }

    @Test
    void testListGroupsConsumer() throws Exception {
        // Create test topic
        topicService.createTopic(admin, "test-topic", 3, 1, null);

        // Create consumer groups
        createConsumerGroup("test-group-1", "test-topic");
        createConsumerGroup("test-group-2", "test-topic");

        // Wait a bit for groups to stabilize
        await().atMost(10, TimeUnit.SECONDS).until(() -> exists("test-group-1"));
        await().atMost(10, TimeUnit.SECONDS).until(() -> exists("test-group-2"));

        Collection<GroupInfo> groups = groupService.listGroups(admin, null);
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
        topicService.createTopic(admin, "test-topic", 3, 1, null);

        // Create consumer group
        createConsumerGroup("consumer-group", "test-topic", "consumer");

        await().atMost(5, TimeUnit.SECONDS).until(() -> exists("consumer-group"));

        // Filter by consumer type
        Collection<GroupInfo> consumerGroups = groupService.listGroups(admin, "consumer");
        assertTrue(consumerGroups.size() >= 1, "Should have at least 1 consumer group");

        // Verify our test group is present
        boolean foundTestGroup = consumerGroups.stream()
                .anyMatch(g -> "consumer-group".equals(g.getGroupId()));
        assertTrue(foundTestGroup, "Should find consumer-group");

        // Filter by share type (should be empty)
        Collection<GroupInfo> shareGroups = groupService.listGroups(admin, "share");
        assertTrue(shareGroups.isEmpty());

        // Filter by stream type (should be empty)
        Collection<GroupInfo> streamGroups = groupService.listGroups(admin, "stream");
        assertTrue(streamGroups.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = { "consumer", "classic" })
    void testDescribeConsumerGroup(String groupProtocol) throws Exception {
        // Create test topic
        topicService.createTopic(admin, "describe-topic", 3, 1, null);

        // Create consumer group
        createConsumerGroup("describe-group", "describe-topic", groupProtocol);

        await().atMost(10, TimeUnit.SECONDS).until(() -> exists("describe-group"));

        GroupInfo group = groupService.describeGroup(admin, "describe-group");
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
        topicService.createTopic(admin, "members-topic", 6, 1, null);

        // Produce some messages
        produceMessages("members-topic", 100);

        // Create multiple consumers in the same group
        createConsumerGroup("multi-member-group", "members-topic");
        createConsumerGroup("multi-member-group", "members-topic");
        createConsumerGroup("multi-member-group", "members-topic");

        // Wait for rebalance
        await().atMost(20, TimeUnit.SECONDS).until(() -> exists("multi-member-group"));

        GroupInfo group = groupService.describeGroup(admin, "multi-member-group");
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
        GroupInfo group = groupService.describeGroup(admin, "non-existent-group");
        assertNull(group);
    }

    @Test
    void testDescribeGroupOffsets() throws Exception {
        // Create test topic
        topicService.createTopic(admin, "offset-topic", 2, 1, null);

        // Produce some messages
        produceMessages("offset-topic", 100);

        // Create consumer and consume some messages
        KafkaConsumer<String, String> consumer = createConsumerGroup("offset-group", "offset-topic");

        // Poll and commit offsets
        consumer.poll(Duration.ofSeconds(5));
        consumer.commitSync();

        await().atMost(5, TimeUnit.SECONDS).until(() -> exists("offset-group"));

        GroupInfo group = groupService.describeGroup(admin, "offset-group");
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
        topicService.createTopic(admin, "no-offset-topic", 2, 1, null);

        // Create consumer but don't poll or commit
        createConsumerGroup("no-offset-group", "no-offset-topic");

        await().atMost(10, TimeUnit.SECONDS).until(() -> exists("no-offset-group"));

        GroupInfo group = groupService.describeGroup(admin, "no-offset-group");
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
     * Helper method to check whether a consumer group is stable
     */
    private boolean exists(String groupId) {
        try {
            GroupState state = admin.describeConsumerGroups(Set.of(groupId)).all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join()
                    .get(groupId)
                    .groupState();
            logger.infof("Consumer group %s state is %s", groupId, state);

            return switch(state) {
                case UNKNOWN, DEAD, EMPTY -> false;
                default -> true;
            };
        } catch (CompletionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                logger.infof("Consumer group %s does not yet exist. Known groups: %s",
                        groupId,
                        admin.listGroups().all().toCompletionStage().toCompletableFuture().join());
            }
            return false;
        }
    }

    /**
     * Helper method to create a consumer group
     */
    private KafkaConsumer<String, String> createConsumerGroup(String groupId, String topic) {
        return createConsumerGroup(groupId, topic, null);
    }

    private KafkaConsumer<String, String> createConsumerGroup(String groupId, String topic, String groupProtocol) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        if (groupProtocol != null) {
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        // Poll once to join the group
        consumer.poll(Duration.ofMillis(1000));

        consumers.add(consumer);
        return consumer;
    }

    /**
     * Helper method to produce test messages
     */
    private void produceMessages(String topic, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)).get();
            }
        }
    }
}
