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
    void testProduceWithPropertyOverride() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "property-override-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with test data
        Path tempFile = Files.createTempFile("produce-property-test", ".txt");
        Files.writeString(tempFile, "message1\nmessage2\n");

        try {
            // Produce with property override
            // Note: This is a smoke test that verifies the --property flag is accepted.
            // It does not verify the property was actually applied to the Kafka client.
            LaunchResult result = launcher.launch("produce", "property-override-topic",
                    "--file", tempFile.toString(),
                    "--property", "linger.ms=100");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages were produced
        List<String> consumed = consumeMessages("property-override-topic", 2);
        assertEquals(2, consumed.size());
        assertEquals("message1", consumed.get(0));
        assertEquals("message2", consumed.get(1));
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

    @Test
    void testProduceWithBase64Value() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "base64-value-topic", 1, 1, Collections.emptyMap());

        // "Hello World" in base64
        LaunchResult result = launcher.launch("produce", "base64-value-topic", "--value", "base64:SGVsbG8gV29ybGQ=");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("base64-value-topic", 1);
        assertEquals(1, records.size());
        assertEquals("Hello World", records.get(0).value());
    }

    @Test
    void testProduceWithHexValue() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "hex-value-topic", 1, 1, Collections.emptyMap());

        // "Hello" in hex
        LaunchResult result = launcher.launch("produce", "hex-value-topic", "--value", "hex:48656c6c6f");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify message was decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("hex-value-topic", 1);
        assertEquals(1, records.size());
        assertEquals("Hello", records.get(0).value());
    }

    @Test
    void testProduceWithBase64Key() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "base64-key-topic", 1, 1, Collections.emptyMap());

        // "test-key" in base64
        LaunchResult result = launcher.launch("produce", "base64-key-topic",
                "--value", "message",
                "--key", "base64:dGVzdC1rZXk=");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify key was decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("base64-key-topic", 1);
        assertEquals(1, records.size());
        assertEquals("test-key", records.get(0).key());
        assertEquals("message", records.get(0).value());
    }

    @Test
    void testProduceWithHexKey() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "hex-key-topic", 1, 1, Collections.emptyMap());

        // "key" in hex (produces valid UTF-8)
        LaunchResult result = launcher.launch("produce", "hex-key-topic",
                "--value", "message",
                "--key", "hex:6b6579");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify key was decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("hex-key-topic", 1);
        assertEquals(1, records.size());
        assertEquals("key", records.get(0).key());
    }

    @Test
    void testProduceWithBase64Header() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "base64-header-topic", 1, 1, Collections.emptyMap());

        // Header value in base64
        LaunchResult result = launcher.launch("produce", "base64-header-topic",
                "--value", "message",
                "--header", "signature=base64:U2lnbmF0dXJl");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify header was decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("base64-header-topic", 1);
        assertEquals(1, records.size());

        Header header = records.get(0).headers().lastHeader("signature");
        assertNotNull(header);
        assertEquals("Signature", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithHexHeader() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "hex-header-topic", 1, 1, Collections.emptyMap());

        // Header value in hex
        LaunchResult result = launcher.launch("produce", "hex-header-topic",
                "--value", "message",
                "--header", "checksum=hex:a1b2c3d4");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify header was decoded correctly (binary data)
        List<ConsumerRecord<String, String>> records = consumeRecords("hex-header-topic", 1);
        assertEquals(1, records.size());

        Header header = records.get(0).headers().lastHeader("checksum");
        assertNotNull(header);
        byte[] expectedValue = new byte[]{(byte) 0xA1, (byte) 0xB2, (byte) 0xC3, (byte) 0xD4};
        assertArrayEquals(expectedValue, header.value());
    }

    @Test
    void testProduceWithMixedEncodings() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "mixed-encoding-topic", 1, 1, Collections.emptyMap());

        // Mix of encodings: hex key, base64 value, plain header
        LaunchResult result = launcher.launch("produce", "mixed-encoding-topic",
                "--key", "hex:6d6978",
                "--value", "base64:VGVzdA==",
                "--header", "type=plain-text");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify all encodings worked correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("mixed-encoding-topic", 1);
        assertEquals(1, records.size());

        ConsumerRecord<String, String> record = records.get(0);

        // Hex key ("mix" in hex)
        assertEquals("mix", record.key());

        // Base64 value
        assertEquals("Test", record.value());

        // Plain text header
        Header header = record.headers().lastHeader("type");
        assertNotNull(header);
        assertEquals("plain-text", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceInvalidBase64() {
        // Invalid base64 characters
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "base64:Invalid!@#$%");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid base64 encoding") ||
                   result.getErrorOutput().contains("Failed to produce messages"));
    }

    @Test
    void testProduceInvalidHexOddLength() {
        // Hex with odd number of characters
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "hex:abc");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("odd number of characters") ||
                   result.getErrorOutput().contains("Failed to produce messages"));
    }

    @Test
    void testProduceInvalidHexCharacters() {
        // Hex with invalid characters
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "hex:gg");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid hex encoding") ||
                   result.getErrorOutput().contains("Failed to produce messages"));
    }

    @Test
    void testProduceBackwardsCompatiblePlainText() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "plain-text-topic", 1, 1, Collections.emptyMap());

        // Plain text without any prefix (backwards compatibility)
        LaunchResult result = launcher.launch("produce", "plain-text-topic",
                "--key", "simple-key",
                "--value", "simple value",
                "--header", "type=text");
        assertEquals(0, result.exitCode());
        assertTrue(result.getOutput().contains("1 messages sent successfully"));

        // Verify everything still works as UTF-8 strings
        List<ConsumerRecord<String, String>> records = consumeRecords("plain-text-topic", 1);
        assertEquals(1, records.size());

        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("simple-key", record.key());
        assertEquals("simple value", record.value());

        Header header = record.headers().lastHeader("type");
        assertNotNull(header);
        assertEquals("text", new String(header.value(), StandardCharsets.UTF_8));
    }

    // ========== Format String (--input) Tests ==========

    @Test
    void testProduceWithInputFormatKeyValue() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-kv-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with key-value pairs
        Path tempFile = Files.createTempFile("produce-format-kv-test", ".txt");
        Files.writeString(tempFile, "key1 value1\nkey2 value2\nkey3 value3\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-kv-topic", "--file", tempFile.toString(),
                    "--input", "%k %v");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("3 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages were produced with correct keys and values
        List<ConsumerRecord<String, String>> records = consumeRecords("format-kv-topic", 3);
        assertEquals(3, records.size());
        assertEquals("key1", records.get(0).key());
        assertEquals("value1", records.get(0).value());
        assertEquals("key2", records.get(1).key());
        assertEquals("value2", records.get(1).value());
        assertEquals("key3", records.get(2).key());
        assertEquals("value3", records.get(2).value());
    }

    @Test
    void testProduceWithInputFormatBase64Key() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-base64-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with base64-encoded keys
        // "test-key" in base64 is "dGVzdC1rZXk="
        Path tempFile = Files.createTempFile("produce-format-base64-test", ".txt");
        Files.writeString(tempFile, "dGVzdC1rZXk= value1\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-base64-topic", "--file", tempFile.toString(),
                    "--input", "%{base64:k} %v");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify key was decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("format-base64-topic", 1);
        assertEquals(1, records.size());
        assertEquals("test-key", records.get(0).key());
        assertEquals("value1", records.get(0).value());
    }

    @Test
    void testProduceWithInputFormatNamedHeader() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-header-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with headers
        Path tempFile = Files.createTempFile("produce-format-header-test", ".txt");
        Files.writeString(tempFile, "key1 value1 content-type=application/json\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-header-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %{h.content-type}");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify message has header
        List<ConsumerRecord<String, String>> records = consumeRecords("format-header-topic", 1);
        assertEquals(1, records.size());
        assertEquals("key1", records.get(0).key());
        assertEquals("value1", records.get(0).value());

        Header header = records.get(0).headers().lastHeader("content-type");
        assertNotNull(header);
        assertEquals("application/json", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithInputFormatGenericHeader() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-generic-header-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with generic headers
        Path tempFile = Files.createTempFile("produce-format-generic-test", ".txt");
        Files.writeString(tempFile, "key1 value1 my-header=data\nkey2 value2 other-header=info\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-generic-header-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %h");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages have headers
        List<ConsumerRecord<String, String>> records = consumeRecords("format-generic-header-topic", 2);
        assertEquals(2, records.size());

        Header header1 = records.get(0).headers().lastHeader("my-header");
        assertNotNull(header1);
        assertEquals("data", new String(header1.value(), StandardCharsets.UTF_8));

        Header header2 = records.get(1).headers().lastHeader("other-header");
        assertNotNull(header2);
        assertEquals("info", new String(header2.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithInputFormatTimestamp() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-timestamp-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with timestamps
        Path tempFile = Files.createTempFile("produce-format-timestamp-test", ".txt");
        Files.writeString(tempFile, "key1 value1 1735401600000\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-timestamp-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %T");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify message has timestamp
        List<ConsumerRecord<String, String>> records = consumeRecords("format-timestamp-topic", 1);
        assertEquals(1, records.size());
        assertEquals(1735401600000L, records.get(0).timestamp());
    }

    @Test
    void testProduceWithInputFormatPartition() throws Exception {
        // Create test topic with multiple partitions
        topicService.createTopic(admin(), "format-partition-topic", 3, 1, Collections.emptyMap());

        // Create temporary file with partition numbers
        Path tempFile = Files.createTempFile("produce-format-partition-test", ".txt");
        Files.writeString(tempFile, "key1 value1 0\nkey2 value2 1\nkey3 value3 2\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-partition-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %p");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("3 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages went to correct partitions
        List<ConsumerRecord<String, String>> records = consumeRecords("format-partition-topic", 3);
        assertEquals(3, records.size());

        // Sort by partition for predictable checking
        records.sort((r1, r2) -> Integer.compare(r1.partition(), r2.partition()));
        assertEquals(0, records.get(0).partition());
        assertEquals(1, records.get(1).partition());
        assertEquals(2, records.get(2).partition());
    }

    @Test
    void testProduceWithInputFormatMixedEncodings() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-mixed-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with mixed encodings
        // "key" in hex: 6b6579
        // "Test" in base64: VGVzdA==
        Path tempFile = Files.createTempFile("produce-format-mixed-test", ".txt");
        Files.writeString(tempFile, "6b6579 VGVzdA== sig=U2lnbmF0dXJl\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-mixed-topic", "--file", tempFile.toString(),
                    "--input", "%{hex:k} %{base64:v} %{base64:h.sig}");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify all encodings were decoded correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("format-mixed-topic", 1);
        assertEquals(1, records.size());
        assertEquals("key", records.get(0).key());
        assertEquals("Test", records.get(0).value());

        Header header = records.get(0).headers().lastHeader("sig");
        assertNotNull(header);
        assertEquals("Signature", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithInputFormatUnicodeDelimiter() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-unicode-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with tab-separated values
        Path tempFile = Files.createTempFile("produce-format-unicode-test", ".txt");
        Files.writeString(tempFile, "key1\tvalue1\nkey2\tvalue2\n");

        try {
            // Use \u0009 for tab character
            LaunchResult result = launcher.launch("produce", "format-unicode-topic", "--file", tempFile.toString(),
                    "--input", "%k\\u0009%v");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify messages were parsed correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("format-unicode-topic", 2);
        assertEquals(2, records.size());
        assertEquals("key1", records.get(0).key());
        assertEquals("value1", records.get(0).value());
        assertEquals("key2", records.get(1).key());
        assertEquals("value2", records.get(1).value());
    }

    @Test
    void testProduceWithInputFormatValueWithSpaces() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-spaces-topic", 1, 1, Collections.emptyMap());

        // Last placeholder should consume rest of line including spaces
        Path tempFile = Files.createTempFile("produce-format-spaces-test", ".txt");
        Files.writeString(tempFile, "key1 value with multiple spaces\nkey2 another value with  spaces\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-spaces-topic", "--file", tempFile.toString(),
                    "--input", "%k %v");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("2 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify values contain spaces
        List<ConsumerRecord<String, String>> records = consumeRecords("format-spaces-topic", 2);
        assertEquals(2, records.size());
        assertEquals("value with multiple spaces", records.get(0).value());
        assertEquals("another value with  spaces", records.get(1).value());
    }

    @Test
    void testProduceInputFormatConflictWithValue() throws Exception {
        // --input cannot be used with --value
        LaunchResult result = launcher.launch("produce", "test-topic", "--value", "test", "--input", "%k %v");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("--input cannot be used with --value"));
    }

    @Test
    void testProduceInputFormatConflictWithKey() throws Exception {
        Path tempFile = Files.createTempFile("produce-conflict-test", ".txt");
        Files.writeString(tempFile, "key1 value1\n");

        try {
            LaunchResult result = launcher.launch("produce", "test-topic", "--file", tempFile.toString(),
                    "--input", "%k %v", "--key", "global-key");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("--input cannot be used with --key"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatConflictWithHeader() throws Exception {
        Path tempFile = Files.createTempFile("produce-conflict-test", ".txt");
        Files.writeString(tempFile, "key1 value1\n");

        try {
            LaunchResult result = launcher.launch("produce", "test-topic", "--file", tempFile.toString(),
                    "--input", "%k %v", "--header", "type=test");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("--input cannot be used with --header"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatConflictWithTimestamp() throws Exception {
        Path tempFile = Files.createTempFile("produce-conflict-test", ".txt");
        Files.writeString(tempFile, "key1 value1\n");

        try {
            LaunchResult result = launcher.launch("produce", "test-topic", "--file", tempFile.toString(),
                    "--input", "%k %v", "--timestamp", "1735401600000");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("--input cannot be used with --timestamp"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatConflictWithPartition() throws Exception {
        Path tempFile = Files.createTempFile("produce-conflict-test", ".txt");
        Files.writeString(tempFile, "key1 value1\n");

        try {
            LaunchResult result = launcher.launch("produce", "test-topic", "--file", tempFile.toString(),
                    "--input", "%k %v", "--partition", "0");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("--input cannot be used with --partition"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatInvalidFormatString() throws Exception {
        Path tempFile = Files.createTempFile("produce-invalid-format-test", ".txt");
        Files.writeString(tempFile, "key1 value1\n");

        try {
            // Format string without placeholders
            LaunchResult result = launcher.launch("produce", "test-topic", "--file", tempFile.toString(),
                    "--input", "just literal text");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("Invalid format string") ||
                       result.getErrorOutput().contains("must contain at least one placeholder"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatMissingDelimiter() throws Exception {
        topicService.createTopic(admin(), "format-error-topic", 1, 1, Collections.emptyMap());

        Path tempFile = Files.createTempFile("produce-missing-delimiter-test", ".txt");
        Files.writeString(tempFile, "keyvalue\n"); // missing space delimiter

        try {
            LaunchResult result = launcher.launch("produce", "format-error-topic", "--file", tempFile.toString(),
                    "--input", "%k %v");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("Failed to parse line") ||
                       result.getErrorOutput().contains("delimiter"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatInvalidHeaderFormat() throws Exception {
        topicService.createTopic(admin(), "format-bad-header-topic", 1, 1, Collections.emptyMap());

        Path tempFile = Files.createTempFile("produce-bad-header-test", ".txt");
        Files.writeString(tempFile, "key1 value1 noequals\n"); // header missing =

        try {
            LaunchResult result = launcher.launch("produce", "format-bad-header-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %h");
            assertEquals(1, result.exitCode());
            assertTrue(result.getErrorOutput().contains("Invalid header format"));
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Test
    void testProduceInputFormatMultipleHeaders() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-multi-header-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with multiple headers
        Path tempFile = Files.createTempFile("produce-format-multi-header-test", ".txt");
        Files.writeString(tempFile, "key1 value1 type=json version=1.0\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-multi-header-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %{h.type} %{h.version}");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify both headers are present
        List<ConsumerRecord<String, String>> records = consumeRecords("format-multi-header-topic", 1);
        assertEquals(1, records.size());

        Header typeHeader = records.get(0).headers().lastHeader("type");
        assertNotNull(typeHeader);
        assertEquals("json", new String(typeHeader.value(), StandardCharsets.UTF_8));

        Header versionHeader = records.get(0).headers().lastHeader("version");
        assertNotNull(versionHeader);
        assertEquals("1.0", new String(versionHeader.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceInputFormatAllFields() throws Exception {
        // Create test topic with multiple partitions
        topicService.createTopic(admin(), "format-all-fields-topic", 3, 1, Collections.emptyMap());

        // Create temporary file with all fields
        Path tempFile = Files.createTempFile("produce-format-all-test", ".txt");
        Files.writeString(tempFile, "mykey myvalue type=json 1735401600000 2\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-all-fields-topic", "--file", tempFile.toString(),
                    "--input", "%k %v %{h.type} %T %p");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify all fields are set correctly
        List<ConsumerRecord<String, String>> records = consumeRecords("format-all-fields-topic", 1);
        assertEquals(1, records.size());

        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("mykey", record.key());
        assertEquals("myvalue", record.value());
        assertEquals(1735401600000L, record.timestamp());
        assertEquals(2, record.partition());

        Header header = record.headers().lastHeader("type");
        assertNotNull(header);
        assertEquals("json", new String(header.value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithInputFormatDuplicateNamedHeaders() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-dup-headers-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with duplicate named headers
        Path tempFile = Files.createTempFile("produce-dup-headers-test", ".txt");
        Files.writeString(tempFile, "key1 value1 tag=v1 tag=v2 tag=v3\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-dup-headers-topic",
                "--file", tempFile.toString(),
                "--input", "%k %v %{h.tag} %{h.tag} %{h.tag}");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify all three "tag" headers are present
        List<ConsumerRecord<String, String>> records = consumeRecords("format-dup-headers-topic", 1);
        assertEquals(1, records.size());

        // Count headers with key "tag"
        List<Header> tagHeaders = new ArrayList<>();
        records.get(0).headers().headers("tag").forEach(tagHeaders::add);
        assertEquals(3, tagHeaders.size(), "Should have 3 'tag' headers");
        assertEquals("v1", new String(tagHeaders.get(0).value(), StandardCharsets.UTF_8));
        assertEquals("v2", new String(tagHeaders.get(1).value(), StandardCharsets.UTF_8));
        assertEquals("v3", new String(tagHeaders.get(2).value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProduceWithInputFormatMultipleGenericHeaders() throws Exception {
        // Create test topic
        topicService.createTopic(admin(), "format-multi-generic-topic", 1, 1, Collections.emptyMap());

        // Create temporary file with multiple generic headers
        Path tempFile = Files.createTempFile("produce-multi-generic-test", ".txt");
        Files.writeString(tempFile, "key1 value1 type=json version=1.0 source=cli\n");

        try {
            LaunchResult result = launcher.launch("produce", "format-multi-generic-topic",
                "--file", tempFile.toString(),
                "--input", "%k %v %h %h %h");
            assertEquals(0, result.exitCode());
            assertTrue(result.getOutput().contains("1 messages sent successfully"));
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // Verify all three headers with different keys are present
        List<ConsumerRecord<String, String>> records = consumeRecords("format-multi-generic-topic", 1);
        assertEquals(1, records.size());

        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("key1", record.key());
        assertEquals("value1", record.value());

        // Verify all three headers are present
        Header typeHeader = record.headers().lastHeader("type");
        assertNotNull(typeHeader);
        assertEquals("json", new String(typeHeader.value(), StandardCharsets.UTF_8));

        Header versionHeader = record.headers().lastHeader("version");
        assertNotNull(versionHeader);
        assertEquals("1.0", new String(versionHeader.value(), StandardCharsets.UTF_8));

        Header sourceHeader = record.headers().lastHeader("source");
        assertNotNull(sourceHeader);
        assertEquals("cli", new String(sourceHeader.value(), StandardCharsets.UTF_8));
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
