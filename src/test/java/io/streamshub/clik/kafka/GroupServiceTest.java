package io.streamshub.clik.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.GroupType;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.kafka.model.GroupInfo;
import io.streamshub.clik.kafka.model.OffsetLagInfo;
import io.streamshub.clik.test.ClikTestBase;
import io.streamshub.clik.test.TestRecordProducer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class GroupServiceTest extends ClikTestBase implements TestRecordProducer {

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
                createConsumer("test-group-1", GroupProtocol.CLASSIC, "test-topic"),
                createConsumer("test-group-2", GroupProtocol.CONSUMER, "test-topic"),
                createShareConsumer("test-group-3", "test-topic"),
                createShareConsumer("test-group-4", "test-topic"),
                createStreamsConsumer("test-group-5", "test-topic"),
                createStreamsConsumer("test-group-6", "test-topic")
        ).join();

        Collection<GroupInfo> groups = groupService.listGroups(admin(), null);
        assertEquals(6, groups.size());

        // Verify our test group IDs are present
        List<String> groupIds = groups.stream()
                .map(GroupInfo::groupId)
                .sorted()
                .toList();
        assertTrue(groupIds.contains("test-group-1"));
        assertTrue(groupIds.contains("test-group-2"));
        assertTrue(groupIds.contains("test-group-3"));
        assertTrue(groupIds.contains("test-group-4"));
        assertTrue(groupIds.contains("test-group-5"));
        assertTrue(groupIds.contains("test-group-6"));

        // Verify basic metadata is present
        for (GroupInfo group : groups) {
            assertNotNull(group.groupId());
            assertNotNull(group.type());
            assertNotNull(group.state());
            assertTrue(group.memberCount() >= 0);
            // Members and offsets should be null for list operation
            assertNull(group.members());
            assertNull(group.offsets());
        }
    }

    @Test
    void testListGroupsFilterByType() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "test-topic", 3, 1, null);

        // Create consumer group
        createConsumer("consumer-group", GroupProtocol.CONSUMER, "test-topic").join();

        // Filter by consumer type
        Collection<GroupInfo> consumerGroups = groupService.listGroups(admin(), "consumer");
        assertTrue(consumerGroups.size() >= 1, "Should have at least 1 consumer group");

        // Verify our test group is present
        boolean foundTestGroup = consumerGroups.stream()
                .anyMatch(g -> "consumer-group".equals(g.groupId()));
        assertTrue(foundTestGroup, "Should find consumer-group");

        // Filter by share type (should be empty)
        Collection<GroupInfo> shareGroups = groupService.listGroups(admin(), "share");
        assertTrue(shareGroups.isEmpty());

        // Filter by stream type (should be empty)
        Collection<GroupInfo> streamGroups = groupService.listGroups(admin(), "stream");
        assertTrue(streamGroups.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        //"STREAMS ,         ,", streams groups cannot be successfully described in Kafka 4.1
    })
    void testDescribeConsumerGroup(GroupType type, GroupProtocol protocol) throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "describe-topic", 3, 1, null);

        // Produce some messages
        produceMessages("describe-topic", 100);

        // Create consumer group
        createConsumer(type, protocol, "describe-group", "describe-topic").join();

        // protocol becomes the type for "classic" consumer groups
        GroupType expectedType = Optional.ofNullable(protocol)
                .map(Enum::name)
                .map(GroupType::parse)
                .orElse(type);

        GroupInfo group = groupService.describeGroup(admin(), "describe-group");
        assertNotNull(group);
        assertEquals("describe-group", group.groupId());
        assertEquals(expectedType, GroupType.parse(group.type()));
        assertNotNull(group.state());
        assertEquals(1, group.memberCount());
        assertNotNull(group.coordinator());
        assertNotNull(group.members());
        assertEquals(1, group.members().size());
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        //"STREAMS ,         ,", streams groups cannot be successfully described in Kafka 4.1
    })
    void testDescribeGroupWithMembers(GroupType type, GroupProtocol protocol) throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "members-topic", 6, 1, null);

        // Produce some messages
        produceMessages("members-topic", 100);

        // Create multiple consumers in the same group
        CompletableFuture.allOf(
                createConsumer(type, protocol, "multi-member-group", "members-topic"),
                createConsumer(type, protocol, "multi-member-group", "members-topic"),
                createConsumer(type, protocol, "multi-member-group", "members-topic")
        ).join();

        GroupInfo group = groupService.describeGroup(admin(), "multi-member-group");
        assertNotNull(group);
        assertEquals("multi-member-group", group.groupId());
        assertEquals(3, group.memberCount());
        assertNotNull(group.members());
        assertEquals(3, group.members().size());

        // Verify member info
        for (var member : group.members()) {
            assertNotNull(member.memberId());
            assertNotNull(member.clientId());
            assertNotNull(member.host());
            assertNotNull(member.assignments());
        }
    }

    @Test
    void testDescribeGroupNotFound() throws Exception {
        GroupInfo group = groupService.describeGroup(admin(), "non-existent-group");
        assertNull(group);
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CONSUMER,",
        "CLASSIC , CLASSIC ,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        //"STREAMS ,         ,", streams groups cannot be successfully described in Kafka 4.1
    })
    void testDescribeGroupOffsets(GroupType type, GroupProtocol protocol) throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "offset-topic", 2, 1, null);

        // Create consumer and consume some messages
        var consumer = createConsumer(type, protocol, "offset-group", "offset-topic")
                .join();

        // Produce some messages
        produceMessages("offset-topic", 100);

        switch (consumer) {
            case Consumer<?, ?> c -> {
                // Poll and commit offsets
                c.poll(Duration.ofSeconds(2));
                c.commitSync();
            }
            case ShareConsumer<?, ?> s -> {
                // Poll and commit offsets
                ConsumerRecords<?, ?> records;

                do {
                    records = s.poll(Duration.ofSeconds(2));

                    if (records.isEmpty()) {
                        logger.debugf("No records received for share consumer");
                        // Produce some messages
                        produceMessages("offset-topic", 100);
                    } else {
                        logger.debugf("Received %d records for share consumer", records.count());
                    }
                } while (records.isEmpty());

                s.commitSync();
            }
            default -> throw new IllegalArgumentException("Unsupported group type: " + type);
        }

        GroupInfo group = groupService.describeGroup(admin(), "offset-group");
        assertNotNull(group);
        assertEquals("offset-group", group.groupId());

        // Verify offsets are present
        List<OffsetLagInfo> offsets = group.offsets();
        assertNotNull(offsets);
        assertFalse(offsets.isEmpty());

        // Verify offset information
        for (OffsetLagInfo offset : offsets) {
            assertEquals("offset-topic", offset.topic());
            assertTrue(offset.partition() >= 0);
            assertNotNull(offset.currentOffset());
            assertNotNull(offset.logEndOffset());
            assertNotNull(offset.lag());
            assertTrue(offset.lag() >= 0);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "CLASSIC , CLASSIC ,",
        "CLASSIC , CONSUMER,",
        "CONSUMER, CONSUMER,",
        "SHARE   ,         ,",
        //"STREAMS ,         ,", streams groups cannot be successfully described in Kafka 4.1
    })
    void testDescribeGroupNoOffsets(GroupType type, GroupProtocol protocol) throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "no-offset-topic", 2, 1, null);

        // Create consumer but don't poll or commit
        createConsumer(type, protocol, "no-offset-group", "no-offset-topic").join();

        GroupInfo group = groupService.describeGroup(admin(), "no-offset-group");
        assertNotNull(group);
        assertEquals("no-offset-group", group.groupId());

        // Offsets may be empty or null for a group with no committed offsets
        List<OffsetLagInfo> offsets = group.offsets();
        if (offsets != null) {
            // If offsets are present, they should be empty or have null current offsets
            assertTrue(offsets.isEmpty() || offsets.get(0).currentOffset() == null);
        }
    }
}
