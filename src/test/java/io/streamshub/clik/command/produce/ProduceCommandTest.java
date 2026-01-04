package io.streamshub.clik.command.produce;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.test.ClikMainTestBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class ProduceCommandTest extends ClikMainTestBase {

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
    @Disabled("Testing with standard input not possible with the launcher")
    void testProduceFromStdin() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "stdin-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-stdin-test", ".txt");
        Files.writeString(tempFile, "message1\nmessage2\nmessage3\n");

        try {
            LaunchResult result = launcher.launch("produce", "stdin-topic", "--file", tempFile.toString());
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("3 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages were produced
        List<String> consumed = consumeMessages("stdin-topic", 3);
        assertEquals(3, consumed.size());
        assertEquals("message1", consumed.get(0));
        assertEquals("message2", consumed.get(1));
        assertEquals("message3", consumed.get(2));
    }

    @Test
    void testProduceFromFile() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "file-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-test", ".txt");
        Files.writeString(tempFile, "file-message1\nfile-message2\n");

        try {
            LaunchResult result = launcher.launch("produce", "file-topic", "--file", tempFile.toString());
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages were produced
        List<String> consumed = consumeMessages("file-topic", 2);
        assertEquals(2, consumed.size());
        assertEquals("file-message1", consumed.get(0));
        assertEquals("file-message2", consumed.get(1));
    }

    @Test
    void testProduceWithKey() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "key-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-key-test", ".txt");
        Files.writeString(tempFile, "message1\nmessage2\n");

        try {
            LaunchResult result = launcher.launch("produce", "key-topic", "--file", tempFile.toString(), "--key", "test-key");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages were produced with the correct key
        List<ConsumerRecord<String, String>> records = consumeRecords("key-topic", 2);
        assertEquals(2, records.size());
        assertEquals("test-key", records.get(0).key());
        assertEquals("test-key", records.get(1).key());
    }

    @Test
    void testProduceWithPartition() throws Exception {
        // Create test topic with multiple partitions
        topicService.createTopic(admin(), "partition-topic", 3, 1, Collections.emptyMap());

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-partition-test", ".txt");
        Files.writeString(tempFile, "message1\n");

        try {
            LaunchResult result = launcher.launch("produce", "partition-topic", "--file", tempFile.toString(), "--partition", "2");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify message was produced to the correct partition
        List<ConsumerRecord<String, String>> records = consumeRecords("partition-topic", 1);
        assertEquals(1, records.size());
        assertEquals(2, records.get(0).partition());
    }

    @Test
    void testProduceNoContext() throws Exception {
        // Delete the context
        contextService.deleteContext("test-context");

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-nocontext-test", ".txt");
        Files.writeString(tempFile, "message1\n");

        try {
            LaunchResult result = launcher.launch("produce", "some-topic", "--file", tempFile.toString());
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("No current context set"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceFileNotFound() {
        LaunchResult result = launcher.launch("produce", "some-topic", "--file", "/nonexistent/file.txt");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("File not found"));
    }

    @Test
    void testProduceMultipleMessages() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "multi-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with 10 messages
        StringBuilder input = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            input.append("message-").append(i).append("\n");
        }
        Path tempFile = Files.createTempFile("produce-multi-test", ".txt");
        Files.writeString(tempFile, input.toString());

        try {
            LaunchResult result = launcher.launch("produce", "multi-topic", "--file", tempFile.toString());
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("10 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify all messages were produced
        List<String> consumed = consumeMessages("multi-topic", 10);
        assertEquals(10, consumed.size());
        for (int i = 0; i < 10; i++) {
            assertEquals("message-" + i, consumed.get(i));
        }
    }

    @Test
    void testProduceEmptyInput() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "empty-topic", 1, 1, Collections.emptyMap());

        // Create empty temporary file
        Path tempFile = Files.createTempFile("produce-empty-test", ".txt");
        Files.writeString(tempFile, "");

        try {
            LaunchResult result = launcher.launch("produce", "empty-topic", "--file", tempFile.toString());
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("0 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /**
     * Helper method to consume messages from a topic
     */
    private List<String> consumeMessages(String topic, int count) {
        List<ConsumerRecord<String, String>> records = consumeRecords(topic, count);
        List<String> messages = new ArrayList<>();
        for (ConsumerRecord<String, String> rec : records) {
            messages.add(rec.value());
        }
        return messages;
    }

    /**
     * Helper method to consume records from a topic
     */
    private List<ConsumerRecord<String, String>> consumeRecords(String topic, int count) {
        Consumer<String, String> consumer = createConsumerGroup("test-consumer-" + System.currentTimeMillis(), topic).join();
        consumer.seekToBeginning(consumer.assignment());

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        while (allRecords.size() < count && System.currentTimeMillis() - startTime < 10000) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> rec : records) {
                allRecords.add(rec);
            }
        }

        close(consumer);
        return allRecords;
    }
}
