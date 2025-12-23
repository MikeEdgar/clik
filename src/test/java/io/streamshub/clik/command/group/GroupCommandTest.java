package io.streamshub.clik.command.group;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikTestBase.Profile.class)
class GroupCommandTest extends ClikTestBase {

    QuarkusMainLauncher launcher;
    List<KafkaConsumer<String, String>> consumers = new ArrayList<>();

    @BeforeEach
    void setUp(QuarkusMainLauncher launcher) {
        this.launcher = launcher;

        // Create and set a test context
        launcher.launch("context", "create", "test-context", "--bootstrap-servers", kafkaBootstrapServers());
        launcher.launch("context", "use", "test-context");
    }

    @Override
    @AfterEach
    protected void tearDown() {
        // Close all consumers
        for (KafkaConsumer<String, String> consumer : consumers) {
            try {
                consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        consumers.clear();

        // Clean up all topics
        LaunchResult listResult = launcher.launch("topic", "list", "-o", "name");
        if (listResult.exitCode() == 0 && !listResult.getOutput().contains("No topics found")) {
            for (String topic : listResult.getOutputStream().stream().map(String::strip).toList()) {
                if (!topic.isEmpty()) {
                    launcher.launch("topic", "delete", topic, "--force");
                }
            }
        }

        super.tearDown();
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
        launcher.launch("topic", "create", "group-test-topic", "--partitions", "3");
        createConsumerGroup("test-group-1", "group-test-topic");
        createConsumerGroup("test-group-2", "group-test-topic");

        // Wait for groups to stabilize
        Thread.sleep(2000);

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
        launcher.launch("topic", "create", "name-test-topic", "--partitions", "2");
        createConsumerGroup("alpha-group", "name-test-topic");
        createConsumerGroup("beta-group", "name-test-topic");
        createConsumerGroup("gamma-group", "name-test-topic");

        // Wait longer for groups to stabilize and be visible
        Thread.sleep(4000);

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
        launcher.launch("topic", "create", "json-group-topic", "--partitions", "2");
        createConsumerGroup("json-group", "json-group-topic");

        Thread.sleep(2000);

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
        launcher.launch("topic", "create", "yaml-group-topic", "--partitions", "2");
        createConsumerGroup("yaml-group", "yaml-group-topic");

        Thread.sleep(2000);

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
        launcher.launch("topic", "create", "filter-topic", "--partitions", "2");
        createConsumerGroup("consumer-group-1", "filter-topic", "classic");

        Thread.sleep(2000);

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
        launcher.launch("topic", "create", "describe-topic", "--partitions", "3");
        createConsumerGroup("describe-group", "describe-topic");

        Thread.sleep(2000);

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
        launcher.launch("topic", "create", "members-topic", "--partitions", "6");
        createConsumerGroup("multi-member-group", "members-topic");
        createConsumerGroup("multi-member-group", "members-topic");
        createConsumerGroup("multi-member-group", "members-topic");

        Thread.sleep(3000); // Wait for rebalance

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
        launcher.launch("topic", "create", "json-describe-topic", "--partitions", "2");
        createConsumerGroup("json-describe-group", "json-describe-topic");

        Thread.sleep(2000);

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
        launcher.launch("topic", "create", "yaml-describe-topic", "--partitions", "2");
        createConsumerGroup("yaml-describe-group", "yaml-describe-topic");

        Thread.sleep(2000);

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
        launcher.launch("context", "delete", "test-context", "--force");

        LaunchResult result = launcher.launch("group", "describe", "some-group");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    @Test
    void testDescribeGroupWithOffsets() throws Exception {
        // Create test topic
        launcher.launch("topic", "create", "offset-test-topic", "--partitions", "2");

        // Produce some messages
        produceMessages("offset-test-topic", 50);

        // Create consumer and consume messages
        KafkaConsumer<String, String> consumer = createConsumerGroup("offset-test-group", "offset-test-topic");

        // Poll and commit offsets
        consumer.poll(Duration.ofSeconds(5));
        consumer.commitSync();

        Thread.sleep(2000);

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

    /**
     * Helper method to create a consumer group
     */
    private KafkaConsumer<String, String> createConsumerGroup(String groupId, String topic) {
        return createConsumerGroup(groupId, topic, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT));
    }

    private KafkaConsumer<String, String> createConsumerGroup(String groupId, String topic, String groupProtocol) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        // Set very long session timeout to keep group alive during tests
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000"); // 60 seconds
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000"); // 20 seconds
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000"); // 120 seconds

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        // Poll multiple times to ensure group join completes
        for (int i = 0; i < 3; i++) {
            consumer.poll(Duration.ofMillis(500));
        }

        consumers.add(consumer);
        return consumer;
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
