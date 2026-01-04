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
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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

    @Test
    void testProduceWithValue() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "value-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "value-topic", "--value", "test message");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was produced
        List<String> consumed = consumeMessages("value-topic", 1);
        assertEquals(1, consumed.size());
        assertEquals("test message", consumed.get(0));
    }

    @Test
    void testProduceWithValueAndKey() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "value-key-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "value-key-topic", "--value", "test message", "--key", "test-key");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was produced with correct key
        List<ConsumerRecord<String, String>> records = consumeRecords("value-key-topic", 1);
        assertEquals(1, records.size());
        assertEquals("test-key", records.get(0).key());
        assertEquals("test message", records.get(0).value());
    }

    @Test
    void testProduceWithValueAndPartition() throws Exception {
        // Create test topic with multiple partitions
        topicService.createTopic(admin(), "value-partition-topic", 3, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "value-partition-topic", "--value", "test message", "--partition", "1");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was produced to correct partition
        List<ConsumerRecord<String, String>> records = consumeRecords("value-partition-topic", 1);
        assertEquals(1, records.size());
        assertEquals(1, records.get(0).partition());
        assertEquals("test message", records.get(0).value());
    }

    @Test
    void testProduceWithSingleHeader() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "single-header-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "single-header-topic", "--value", "test message",
                "--header", "content-type=application/json");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was produced with header
        List<ConsumerRecord<String, String>> records = consumeRecords("single-header-topic", 1);
        assertEquals(1, records.size());

        Header header = records.get(0).headers().lastHeader("content-type");
        assertNotNull(header);
        assertEquals("application/json", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithMultipleHeaders() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "multi-header-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "multi-header-topic", "--value", "test message",
                "--header", "content-type=application/json",
                "--header", "version=1.0",
                "--header", "source=cli");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was produced with all headers
        List<ConsumerRecord<String, String>> records = consumeRecords("multi-header-topic", 1);
        assertEquals(1, records.size());

        Header contentType = records.get(0).headers().lastHeader("content-type");
        assertNotNull(contentType);
        assertEquals("application/json", new String(contentType.value(), StandardCharsets.UTF_8));

        Header version = records.get(0).headers().lastHeader("version");
        assertNotNull(version);
        assertEquals("1.0", new String(version.value(), StandardCharsets.UTF_8));

        Header source = records.get(0).headers().lastHeader("source");
        assertNotNull(source);
        assertEquals("cli", new String(source.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithDuplicateHeaderKeys() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "dup-header-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "dup-header-topic", "--value", "test message",
                "--header", "tag=v1",
                "--header", "tag=v2");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message has duplicate header keys (Kafka supports this)
        List<ConsumerRecord<String, String>> records = consumeRecords("dup-header-topic", 1);
        assertEquals(1, records.size());

        List<Header> tagHeaders = new ArrayList<>();
        records.get(0).headers().headers("tag").forEach(tagHeaders::add);
        assertEquals(2, tagHeaders.size());
        assertEquals("v1", new String(tagHeaders.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("v2", new String(tagHeaders.get(1).value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithHeadersFromFile() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "file-header-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-header-test", ".txt");
        Files.writeString(tempFile, "message1\nmessage2\n");

        try {
            LaunchResult result = launcher.launch("produce", "file-header-topic", "--file", tempFile.toString(),
                    "--header", "batch=true");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify all messages have the header
        List<ConsumerRecord<String, String>> records = consumeRecords("file-header-topic", 2);
        assertEquals(2, records.size());

        for (ConsumerRecord<String, String> record : records) {
            Header header = record.headers().lastHeader("batch");
            assertNotNull(header);
            assertEquals("true", new String(header.value(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void testProduceWithTimestampEpochMillis() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "timestamp-epoch-topic", 1, 1, Collections.emptyMap());

        long expectedTimestamp = 1735401600000L; // 2024-12-28T12:00:00Z
        LaunchResult result = launcher.launch("produce", "timestamp-epoch-topic", "--value", "test message",
                "--timestamp", String.valueOf(expectedTimestamp));
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message has correct timestamp
        List<ConsumerRecord<String, String>> records = consumeRecords("timestamp-epoch-topic", 1);
        assertEquals(1, records.size());
        assertEquals(expectedTimestamp, records.get(0).timestamp());
    }

    @Test
    void testProduceWithTimestampISO8601() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "timestamp-iso-topic", 1, 1, Collections.emptyMap());

        String isoTimestamp = "2026-01-04T12:00:00Z";
        long expectedTimestamp = Instant.parse(isoTimestamp).toEpochMilli();

        LaunchResult result = launcher.launch("produce", "timestamp-iso-topic", "--value", "test message",
                "--timestamp", isoTimestamp);
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message has correct timestamp
        List<ConsumerRecord<String, String>> records = consumeRecords("timestamp-iso-topic", 1);
        assertEquals(1, records.size());
        assertEquals(expectedTimestamp, records.get(0).timestamp());
    }

    @Test
    void testProduceWithValueHeadersAndTimestamp() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "combined-topic", 1, 1, Collections.emptyMap());

        String isoTimestamp = "2026-01-04T15:30:00Z";
        long expectedTimestamp = Instant.parse(isoTimestamp).toEpochMilli();

        LaunchResult result = launcher.launch("produce", "combined-topic",
                "--value", "test message",
                "--key", "test-key",
                "--header", "type=test",
                "--header", "version=1.0",
                "--timestamp", isoTimestamp);
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message has all attributes
        List<ConsumerRecord<String, String>> records = consumeRecords("combined-topic", 1);
        assertEquals(1, records.size());

        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("test-key", record.key());
        assertEquals("test message", record.value());
        assertEquals(expectedTimestamp, record.timestamp());

        Header typeHeader = record.headers().lastHeader("type");
        assertNotNull(typeHeader);
        assertEquals("test", new String(typeHeader.value(), StandardCharsets.UTF_8));

        Header versionHeader = record.headers().lastHeader("version");
        assertNotNull(versionHeader);
        assertEquals("1.0", new String(versionHeader.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceMutualExclusivityValueAndFile() throws Exception {
        // Create temporary file
        Path tempFile = Files.createTempFile("produce-mutex-test", ".txt");
        Files.writeString(tempFile, "message\n");

        try {
            LaunchResult result = launcher.launch("produce", "test-topic", "--value", "test", "--file", tempFile.toString());
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("mutually exclusive"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceMutualExclusivityValueAndInteractive() {
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "test", "--interactive");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("mutually exclusive"));
    }

    @Test
    void testProduceInvalidHeaderFormat() {
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "test", "--header", "invalidheader");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid header format") ||
                   result.getErrorOutput().contains("Expected format: key=value"));
    }

    @Test
    void testProduceInvalidHeaderFormatEmptyKey() {
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "test", "--header", "=value");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid header format") ||
                   result.getErrorOutput().contains("Expected format: key=value"));
    }

    @Test
    void testProduceInvalidTimestampFormat() {
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "test", "--timestamp", "invalid-timestamp");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid timestamp format") ||
                   result.getErrorOutput().contains("Expected epoch milliseconds or ISO-8601"));
    }

    @Test
    void testProduceHeaderWithEqualsInValue() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "header-equals-topic", 1, 1, Collections.emptyMap());

        // Header value contains equals sign (should only split on first equals)
        LaunchResult result = launcher.launch("produce", "header-equals-topic", "--value", "test message",
                "--header", "equation=x=y+z");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify header value is correct
        List<ConsumerRecord<String, String>> records = consumeRecords("header-equals-topic", 1);
        assertEquals(1, records.size());

        Header header = records.get(0).headers().lastHeader("equation");
        assertNotNull(header);
        assertEquals("x=y+z", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceEmptyValue() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "empty-value-topic", 1, 1, Collections.emptyMap());

        LaunchResult result = launcher.launch("produce", "empty-value-topic", "--value", "");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify empty message was produced
        List<String> consumed = consumeMessages("empty-value-topic", 1);
        assertEquals(1, consumed.size());
        assertEquals("", consumed.get(0));
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
