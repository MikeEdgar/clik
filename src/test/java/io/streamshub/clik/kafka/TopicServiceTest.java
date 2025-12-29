package io.streamshub.clik.kafka;

import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.kafka.model.TopicInfo;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class TopicServiceTest extends ClikTestBase {

    @Inject
    TopicService topicService;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @Override
    protected String kafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    @Test
    void testCreateTopic() throws Exception {
        topicService.createTopic(admin(), "test-topic", 3, 1, null);

        Set<String> topics = topicService.listTopics(admin(), false);
        assertTrue(topics.contains("test-topic"));

        TopicInfo info = topicService.describeTopic(admin(), "test-topic");
        assertEquals("test-topic", info.getName());
        assertEquals(3, info.getPartitions());
        assertEquals(1, info.getReplicationFactor());
        assertFalse(info.isInternal());
    }

    @Test
    void testCreateTopicWithConfig() throws Exception {
        Map<String, String> configs = Map.of(
                "retention.ms", "86400000",
                "cleanup.policy", "delete"
        );

        topicService.createTopic(admin(), "test-topic-config", 1, 1, configs);

        TopicInfo info = topicService.describeTopic(admin(), "test-topic-config");
        assertEquals("test-topic-config", info.getName());
        assertEquals("86400000", info.getConfig().get("retention.ms"));
        assertEquals("delete", info.getConfig().get("cleanup.policy"));
    }

    @Test
    void testCreateTopicAlreadyExists() throws Exception {
        topicService.createTopic(admin(), "duplicate-topic", 1, 1, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                topicService.createTopic(admin(), "duplicate-topic", 1, 1, null));

        assertTrue(exception.getMessage().contains("already exists"));
    }

    @Test
    void testListTopicsEmpty() throws Exception {
        Set<String> topics = topicService.listTopics(admin(), false);
        assertTrue(topics.isEmpty());
    }

    @Test
    void testListTopics() throws Exception {
        topicService.createTopic(admin(), "topic1", 1, 1, null);
        topicService.createTopic(admin(), "topic2", 1, 1, null);
        topicService.createTopic(admin(), "topic3", 1, 1, null);

        Set<String> topics = topicService.listTopics(admin(), false);
        assertEquals(3, topics.size());
        assertTrue(topics.contains("topic1"));
        assertTrue(topics.contains("topic2"));
        assertTrue(topics.contains("topic3"));
    }

    @Test
    void testDescribeTopic() throws Exception {
        topicService.createTopic(admin(), "describe-topic", 5, 1, null);

        TopicInfo info = topicService.describeTopic(admin(), "describe-topic");
        assertNotNull(info);
        assertEquals("describe-topic", info.getName());
        assertEquals(5, info.getPartitions());
        assertEquals(1, info.getReplicationFactor());
        assertFalse(info.isInternal());
        assertNotNull(info.getPartitionDetails());
        assertEquals(5, info.getPartitionDetails().size());
    }

    @Test
    void testDescribeTopics() throws Exception {
        topicService.createTopic(admin(), "multi-topic1", 2, 1, null);
        topicService.createTopic(admin(), "multi-topic2", 3, 1, null);

        Map<String, TopicInfo> topics = topicService.describeTopics(admin(), List.of("multi-topic1", "multi-topic2"));
        assertEquals(2, topics.size());

        TopicInfo topic1 = topics.get("multi-topic1");
        assertNotNull(topic1);
        assertEquals("multi-topic1", topic1.getName());
        assertEquals(2, topic1.getPartitions());

        TopicInfo topic2 = topics.get("multi-topic2");
        assertNotNull(topic2);
        assertEquals("multi-topic2", topic2.getName());
        assertEquals(3, topic2.getPartitions());
    }

    @Test
    void testAlterTopicConfig() throws Exception {
        topicService.createTopic(admin(), "alter-topic", 1, 1, null);

        Map<String, String> newConfigs = Map.of(
                "retention.ms", "3600000",
                "max.message.bytes", "2000000"
        );

        topicService.alterTopicConfig(admin(), "alter-topic", newConfigs, null);

        TopicInfo info = topicService.describeTopic(admin(), "alter-topic");
        assertEquals("3600000", info.getConfig().get("retention.ms"));
        assertEquals("2000000", info.getConfig().get("max.message.bytes"));
    }

    @Test
    void testAlterTopicConfigDelete() throws Exception {
        Map<String, String> initialConfigs = Map.of(
                "retention.ms", "3600000",
                "max.message.bytes", "2000000"
        );
        topicService.createTopic(admin(), "alter-delete-topic", 1, 1, initialConfigs);

        TopicInfo info = topicService.describeTopic(admin(), "alter-delete-topic");
        assertEquals("3600000", info.getConfig().get("retention.ms"));
        assertEquals("2000000", info.getConfig().get("max.message.bytes"));

        // Delete one config
        topicService.alterTopicConfig(admin(), "alter-delete-topic", null, List.of("max.message.bytes"));

        info = topicService.describeTopic(admin(), "alter-delete-topic");
        assertEquals("3600000", info.getConfig().get("retention.ms"));
        assertNull(info.getConfig().get("max.message.bytes"));
    }

    @Test
    void testAlterTopicConfigSetAndDelete() throws Exception {
        Map<String, String> initialConfigs = Map.of(
                "retention.ms", "3600000",
                "max.message.bytes", "2000000"
        );
        topicService.createTopic(admin(), "alter-both-topic", 1, 1, initialConfigs);

        // Set a new config and delete an existing one
        Map<String, String> newConfigs = Map.of("compression.type", "snappy");
        topicService.alterTopicConfig(admin(), "alter-both-topic", newConfigs, List.of("max.message.bytes"));

        TopicInfo info = topicService.describeTopic(admin(), "alter-both-topic");
        assertEquals("3600000", info.getConfig().get("retention.ms"));
        assertEquals("snappy", info.getConfig().get("compression.type"));
        assertNull(info.getConfig().get("max.message.bytes"));
    }

    @Test
    void testDeleteTopic() throws Exception {
        topicService.createTopic(admin(), "delete-topic", 1, 1, null);
        assertTrue(topicService.listTopics(admin(), false).contains("delete-topic"));

        topicService.deleteTopic(admin(), "delete-topic");

        assertFalse(topicService.listTopics(admin(), false).contains("delete-topic"));
    }

    @Test
    void testDeleteMultipleTopics() throws Exception {
        topicService.createTopic(admin(), "delete-topic1", 1, 1, null);
        topicService.createTopic(admin(), "delete-topic2", 1, 1, null);
        topicService.createTopic(admin(), "delete-topic3", 1, 1, null);

        Set<String> topics = topicService.listTopics(admin(), false);
        assertEquals(3, topics.size());

        topicService.deleteTopics(admin(), List.of("delete-topic1", "delete-topic2", "delete-topic3"));

        topics = topicService.listTopics(admin(), false);
        assertTrue(topics.isEmpty());
    }

    @Test
    void testIncreasePartitions() throws Exception {
        topicService.createTopic(admin(), "partition-test", 3, 1, null);

        TopicInfo beforeInfo = topicService.describeTopic(admin(), "partition-test");
        assertEquals(3, beforeInfo.getPartitions());

        topicService.increasePartitions(admin(), "partition-test", 6);

        TopicInfo afterInfo = topicService.describeTopic(admin(), "partition-test");
        assertEquals(6, afterInfo.getPartitions());
    }
}
