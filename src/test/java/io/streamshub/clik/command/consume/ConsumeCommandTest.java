package io.streamshub.clik.command.consume;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class ConsumeCommandTest extends ClikMainTestBase {

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
    void testConsumeStandalone() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "standalone-topic", 1, 1, Collections.emptyMap());
        produceMessages("standalone-topic", "msg1", "msg2", "msg3");

        // Consume messages
        LaunchResult result = launcher.launch("consume", "standalone-topic", "--from-beginning", "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("msg1"));
        assertTrue(output.contains("msg2"));
        assertTrue(output.contains("msg3"));
    }

    @Test
    void testConsumeWithGroup() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "group-topic", 1, 1, Collections.emptyMap());
        produceMessages("group-topic", "groupmsg1", "groupmsg2");

        // Consume with group ID
        LaunchResult result = launcher.launch("consume", "group-topic",
                "--group", "test-consume-group",
                "--from-beginning",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("groupmsg1"));
        assertTrue(output.contains("groupmsg2"));
    }

    @Test
    void testConsumeFromBeginning() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "begin-topic", 1, 1, Collections.emptyMap());
        produceMessages("begin-topic", "first", "second", "third");

        // Consume from beginning
        LaunchResult result = launcher.launch("consume", "begin-topic",
                "--from-beginning",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("first"));
        assertTrue(output.contains("second"));
        assertTrue(output.contains("third"));
    }

    @Test
    void testConsumeFromEnd() throws Exception {
        // Create topic and produce some initial messages
        topicService.createTopic(admin(), "end-topic", 1, 1, Collections.emptyMap());
        produceMessages("end-topic", "old1", "old2");

        // Produce new messages after a short delay
        Thread.sleep(100);
        produceMessages("end-topic", "new1", "new2");

        // Consume from end (should not get the old messages)
        LaunchResult result = launcher.launch("consume", "end-topic",
                "--from-end",
                "--timeout", "1000");
        assertEquals(0, result.exitCode());

        // Output should show "No messages consumed" since we start from end
        // and no new messages were produced after the consume started
        String output = result.getOutput();
        assertTrue(output.contains("No messages consumed"));
    }

    @Test
    void testConsumeSpecificOffset() throws Exception {
        // Create topic with multiple partitions
        topicService.createTopic(admin(), "offset-topic", 2, 1, Collections.emptyMap());
        // Produce messages to partition 0
        produceMessagesToPartition("offset-topic", 0, "p0-msg0", "p0-msg1", "p0-msg2", "p0-msg3");

        // Consume from offset 2 in partition 0
        LaunchResult result = launcher.launch("consume", "offset-topic",
                "--partition", "0",
                "--from-offset", "2",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        // Should get messages from offset 2 onward
        assertFalse(output.contains("p0-msg0"));
        assertFalse(output.contains("p0-msg1"));
        assertTrue(output.contains("p0-msg2"));
        assertTrue(output.contains("p0-msg3"));
    }

    @Test
    void testConsumeSpecificPartition() throws Exception {
        // Create topic with multiple partitions
        topicService.createTopic(admin(), "partition-topic", 3, 1, Collections.emptyMap());
        // Produce messages to specific partitions
        produceMessagesToPartition("partition-topic", 0, "p0-msg");
        produceMessagesToPartition("partition-topic", 1, "p1-msg");
        produceMessagesToPartition("partition-topic", 2, "p2-msg");

        // Consume only from partition 1
        LaunchResult result = launcher.launch("consume", "partition-topic",
                "--partition", "1",
                "--from-beginning",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertFalse(output.contains("p0-msg"));
        assertTrue(output.contains("p1-msg"));
        assertFalse(output.contains("p2-msg"));
    }

    @Test
    void testConsumeMaxMessages() throws Exception {
        // Create topic and produce more messages than we want to consume
        topicService.createTopic(admin(), "max-topic", 1, 1, Collections.emptyMap());
        produceMessages("max-topic", "m1", "m2", "m3", "m4", "m5");

        // Consume only 3 messages
        LaunchResult result = launcher.launch("consume", "max-topic",
                "--from-beginning",
                "--max-messages", "3",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        // Count the number of messages in output
        String output = result.getOutput();
        int messageCount = 0;
        if (output.contains("m1")) messageCount++;
        if (output.contains("m2")) messageCount++;
        if (output.contains("m3")) messageCount++;
        // Should not contain m4 or m5
        assertFalse(output.contains("m4"));
        assertFalse(output.contains("m5"));
        assertTrue(messageCount <= 3);
    }

    @Test
    void testConsumeTableFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "table-topic", 1, 1, Collections.emptyMap());
        produceMessages("table-topic", "table-msg1", "table-msg2");

        // Consume in table format (default)
        LaunchResult result = launcher.launch("consume", "table-topic",
                "--from-beginning",
                "-o", "table",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("PARTITION"));
        assertTrue(output.contains("OFFSET"));
        assertTrue(output.contains("VALUE"));
        assertTrue(output.contains("table-msg1"));
        assertTrue(output.contains("table-msg2"));
    }

    @Test
    void testConsumeJsonFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "json-topic", 1, 1, Collections.emptyMap());
        produceMessages("json-topic", "json-msg");

        // Consume in JSON format
        LaunchResult result = launcher.launch("consume", "json-topic",
                "--from-beginning",
                "-o", "json",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("\"partition\""));
        assertTrue(output.contains("\"offset\""));
        assertTrue(output.contains("\"value\""));
        assertTrue(output.contains("\"json-msg\""));
    }

    @Test
    void testConsumeYamlFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "yaml-topic", 1, 1, Collections.emptyMap());
        produceMessages("yaml-topic", "yaml-msg");

        // Consume in YAML format
        LaunchResult result = launcher.launch("consume", "yaml-topic",
                "--from-beginning",
                "-o", "yaml",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("partition:"));
        assertTrue(output.contains("offset:"));
        assertTrue(output.contains("value:"));
        assertTrue(output.contains("yaml-msg"));
    }

    @Test
    void testConsumeValueFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "value-topic", 1, 1, Collections.emptyMap());
        produceMessages("value-topic", "value-msg1", "value-msg2");

        // Consume in value-only format
        LaunchResult result = launcher.launch("consume", "value-topic",
                "--from-beginning",
                "-o", "value",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        // Should only contain values, no metadata
        assertFalse(output.contains("PARTITION"));
        assertFalse(output.contains("OFFSET"));
        assertTrue(output.contains("value-msg1"));
        assertTrue(output.contains("value-msg2"));
    }

    @Test
    void testConsumeNoMessages() throws Exception {
        // Create empty topic
        topicService.createTopic(admin(), "empty-topic", 1, 1, Collections.emptyMap());

        // Try to consume
        LaunchResult result = launcher.launch("consume", "empty-topic",
                "--from-beginning",
                "--timeout", "2000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("No messages consumed"));
    }

    @Test
    void testConsumeNonExistentTopic() {
        // Try to consume from non-existent topic
        LaunchResult result = launcher.launch("consume", "nonexistent-topic",
                "--from-beginning",
                "--timeout", "2000");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Failed to consume messages"));
    }

    @Test
    void testConsumeNoContext() {
        // Delete the context
        contextService.deleteContext("test-context");

        LaunchResult result = launcher.launch("consume", "some-topic");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("No current context set"));
    }

    /**
     * Helper method to produce test messages
     */
    private void produceMessages(String topic, String... messages) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String message : messages) {
                producer.send(new ProducerRecord<>(topic, message)).get();
            }
        }
    }

    /**
     * Helper method to produce messages to a specific partition
     */
    private void produceMessagesToPartition(String topic, int partition, String... messages) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String message : messages) {
                producer.send(new ProducerRecord<>(topic, partition, null, message)).get();
            }
        }
    }
}
