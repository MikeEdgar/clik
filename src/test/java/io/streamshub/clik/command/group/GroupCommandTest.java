package io.streamshub.clik.command.group;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class GroupCommandTest extends ClikMainTestBase {

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
