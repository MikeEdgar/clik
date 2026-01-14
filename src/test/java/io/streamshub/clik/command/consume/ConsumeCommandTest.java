package io.streamshub.clik.command.consume;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
import io.streamshub.clik.test.TestRecordProducer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusMainTest
@TestProfile(ClikMainTestBase.Profile.class)
class ConsumeCommandTest extends ClikMainTestBase implements TestRecordProducer {

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
        String output = result.getErrorOutput();
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

        // Consume in value-only format using custom format string
        LaunchResult result = launcher.launch("consume", "value-topic",
                "--from-beginning",
                "-o", "%v",
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

        String output = result.getErrorOutput();
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

    @Test
    void testConsumeWithPropertyOverride() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "property-override-topic", 1, 1, Collections.emptyMap());
        produceMessages("property-override-topic", "msg1", "msg2");

        // Consume with property override
        // Note: This is a smoke test that verifies the --property flag is accepted.
        // It does not verify the property was actually applied to the Kafka client.
        LaunchResult result = launcher.launch("consume", "property-override-topic",
                "--from-beginning",
                "--timeout", "3000",
                "--property", "max.poll.records=1");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("msg1"));
        assertTrue(output.contains("msg2"));
    }

    @Test
    void testConsumeGroupWithPartitionError() throws Exception {
        // Create topic with multiple partitions
        topicService.createTopic(admin(), "group-partition-error-topic", 3, 1, Collections.emptyMap());

        // Try to use --group with --partition (should fail)
        LaunchResult result = launcher.launch("consume", "group-partition-error-topic",
                "--group", "test-group",
                "--partition", "0",
                "--from-beginning");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("--groupId cannot be used with --partition"));
    }

    @Test
    void testConsumeWithHeadersJson() throws Exception {
        // Create topic and produce messages with headers
        topicService.createTopic(admin(), "headers-json-topic", 1, 1, Collections.emptyMap());

        var headers = List.of(
                TestHeader.of("content-type", "application/json"),
                TestHeader.of("version", "1.0")
        );

        produceMessagesWithHeaders("headers-json-topic", headers, "test message");

        // Consume with JSON output
        LaunchResult result = launcher.launch("consume", "headers-json-topic",
                "--from-beginning",
                "--output", "json",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("\"headers\""));
        assertTrue(output.contains("\"key\":\"content-type\""));
        assertTrue(output.contains("\"value\":\"application/json\""));
        assertTrue(output.contains("\"key\":\"version\""));
        assertTrue(output.contains("\"value\":\"1.0\""));
    }

    @Test
    void testConsumeWithHeadersYaml() throws Exception {
        // Create topic and produce messages with headers
        topicService.createTopic(admin(), "headers-yaml-topic", 1, 1, Collections.emptyMap());

        var headers = List.of(
                TestHeader.of("source", "cli"),
                TestHeader.of("environment", "test")
        );

        produceMessagesWithHeaders("headers-yaml-topic", headers, "yaml test message");

        // Consume with YAML output
        LaunchResult result = launcher.launch("consume", "headers-yaml-topic",
                "--from-beginning",
                "--output", "yaml",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("headers:"));
        assertTrue(output.contains("key: \"source\""));
        assertTrue(output.contains("value: \"cli\""));
        assertTrue(output.contains("key: \"environment\""));
        assertTrue(output.contains("value: \"test\""));
    }

    @Test
    void testConsumeWithDuplicateHeadersJson() throws Exception {
        // Create topic and produce messages with duplicate header keys
        topicService.createTopic(admin(), "dup-headers-json-topic", 1, 1, Collections.emptyMap());

        var headers = List.of(
                TestHeader.of("tag", "v1"),
                TestHeader.of("tag", "v2"),
                TestHeader.of("tag", "v3")
        );

        produceMessagesWithHeaders("dup-headers-json-topic", headers, "duplicate header test");

        // Consume with JSON output
        LaunchResult result = launcher.launch("consume", "dup-headers-json-topic",
                "--from-beginning",
                "--output", "json",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("\"headers\""));
        // All three tag headers should be present
        int tagCount = countOccurrences(output, "\"key\":\"tag\"");
        assertEquals(3, tagCount);
        assertTrue(output.contains("\"value\":\"v1\""));
        assertTrue(output.contains("\"value\":\"v2\""));
        assertTrue(output.contains("\"value\":\"v3\""));
    }

    @Test
    void testConsumeWithDuplicateHeadersYaml() throws Exception {
        // Create topic and produce messages with duplicate header keys
        topicService.createTopic(admin(), "dup-headers-yaml-topic", 1, 1, Collections.emptyMap());

        var headers = List.of(
                TestHeader.of("label", "alpha"),
                TestHeader.of("label", "beta")
        );

        produceMessagesWithHeaders("dup-headers-yaml-topic", headers, "yaml duplicate test");

        // Consume with YAML output
        LaunchResult result = launcher.launch("consume", "dup-headers-yaml-topic",
                "--from-beginning",
                "--output", "yaml",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("headers:"));
        int labelCount = countOccurrences(output, "key: \"label\"");
        assertEquals(2, labelCount);
        assertTrue(output.contains("value: \"alpha\""));
        assertTrue(output.contains("value: \"beta\""));
    }

    @Test
    void testConsumeWithNoHeadersJson() throws Exception {
        // Create topic and produce messages without headers
        topicService.createTopic(admin(), "no-headers-json-topic", 1, 1, Collections.emptyMap());
        produceMessages("no-headers-json-topic", "message without headers");

        // Consume with JSON output
        LaunchResult result = launcher.launch("consume", "no-headers-json-topic",
                "--from-beginning",
                "--output", "json",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        // Headers field should exist but be an empty array
        assertTrue(output.contains("\"headers\":[]"));
    }

    @Test
    void testConsumeWithNoHeadersYaml() throws Exception {
        // Create topic and produce messages without headers
        topicService.createTopic(admin(), "no-headers-yaml-topic", 1, 1, Collections.emptyMap());
        produceMessages("no-headers-yaml-topic", "message without headers");

        // Consume with YAML output
        LaunchResult result = launcher.launch("consume", "no-headers-yaml-topic",
                "--from-beginning",
                "--output", "yaml",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        // Headers field should exist but be empty
        assertTrue(output.contains("headers: []"));
    }

    /**
     * Helper method to count occurrences of a substring
     */
    private int countOccurrences(String text, String substring) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(substring, index)) != -1) {
            count++;
            index += substring.length();
        }
        return count;
    }

    // ========== Format String Tests ==========

    @Test
    void testConsumeWithSimpleFormatString() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "format-simple-topic", 1, 1, Collections.emptyMap());
        produceMessages("format-simple-topic", "value1", "value2");

        // Consume with simple format string
        LaunchResult result = launcher.launch("consume", "format-simple-topic",
                "--from-beginning",
                "--output", "%p:%o %v",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("0:0 value1"));
        assertTrue(output.contains("0:1 value2"));
    }

    @Test
    void testConsumeWithKeyValueFormat() throws Exception {
        // Create topic and produce messages with keys
        topicService.createTopic(admin(), "format-kv-topic", 1, 1, Collections.emptyMap());

        produceRecords(
                TestRecord.of("format-kv-topic", "key1", "value1"),
                TestRecord.of("format-kv-topic", "key2", "value2")
        );

        // Consume with key=value format
        LaunchResult result = launcher.launch("consume", "format-kv-topic",
                "--from-beginning",
                "--output", "%k=%v",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("key1=value1"));
        assertTrue(output.contains("key2=value2"));
    }

    @Test
    void testConsumeWithBase64EncodedFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "format-base64-topic", 1, 1, Collections.emptyMap());
        produceMessages("format-base64-topic", "test");

        // Consume with base64 encoded value
        LaunchResult result = launcher.launch("consume", "format-base64-topic",
                "--from-beginning",
                "--output", "%{base64:v}",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        // "test" in base64 is "dGVzdA=="
        assertTrue(output.contains("dGVzdA=="));
    }

    @Test
    void testConsumeWithHeadersInFormat() throws Exception {
        // Create topic and produce messages with headers
        topicService.createTopic(admin(), "format-headers-topic", 1, 1, Collections.emptyMap());

        var headers = List.of(
                TestHeader.of("type", "test"),
                TestHeader.of("version", "1.0")
        );

        produceMessagesWithHeaders("format-headers-topic", headers, "message1");

        // Consume with headers in format
        LaunchResult result = launcher.launch("consume", "format-headers-topic",
                "--from-beginning",
                "--output", "%v headers=%h",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("message1 headers=type=test version=1.0"));
    }

    @Test
    void testConsumeWithNamedHeaderInFormat() throws Exception {
        // Create topic and produce messages with headers
        topicService.createTopic(admin(), "format-named-header-topic", 1, 1, Collections.emptyMap());

        var headers = List.of(TestHeader.of("content-type", "application/json"));
        produceMessagesWithHeaders("format-named-header-topic", headers, "json-data");

        // Consume with named header in format
        LaunchResult result = launcher.launch("consume", "format-named-header-topic",
                "--from-beginning",
                "--output", "%v [%{h[content-type]}]",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("json-data [content-type=application/json]"));
    }

    @Test
    void testConsumeWithJsonLikeFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "format-json-topic", 1, 1, Collections.emptyMap());
        produceMessages("format-json-topic", "data");

        // Consume with JSON-like custom format
        LaunchResult result = launcher.launch("consume", "format-json-topic",
                "--from-beginning",
                "--output", "{\"partition\":%p,\"offset\":%o,\"value\":\"%v\"}",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("{\"partition\":0,\"offset\":0,\"value\":\"data\"}"));
    }

    @Test
    void testConsumeWithCsvFormat() throws Exception {
        // Create topic and produce messages
        topicService.createTopic(admin(), "format-csv-topic", 1, 1, Collections.emptyMap());

        produceRecords(TestRecord.of("format-csv-topic", "key1", "value1"));

        // Consume with CSV format
        LaunchResult result = launcher.launch("consume", "format-csv-topic",
                "--from-beginning",
                "--output", "\"%k\",\"%v\",%p,%o",
                "--timeout", "3000");
        assertEquals(0, result.exitCode());

        String output = result.getOutput();
        assertTrue(output.contains("\"key1\",\"value1\",0,0"));
    }

    @Test
    void testConsumeWithInvalidFormatString() {
        // Try to consume with invalid format string (no placeholders)
        LaunchResult result = launcher.launch("consume", "some-topic",
                "--from-beginning",
                "--output", "just literal text",
                "--timeout", "2000");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid output format"));
    }

    @Test
    void testConsumeWithMalformedFormatString() {
        // Try to consume with malformed format string
        LaunchResult result = launcher.launch("consume", "some-topic",
                "--from-beginning",
                "--output", "%{unclosed",
                "--timeout", "2000");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid output format"));
    }

    @Test
    void testConsumeFormatStringWithUnknownPlaceholder() {
        // Try to consume with unknown placeholder
        LaunchResult result = launcher.launch("consume", "some-topic",
                "--from-beginning",
                "--output", "%x %v",
                "--timeout", "2000");
        assertEquals(1, result.exitCode());
        assertTrue(result.getErrorOutput().contains("Invalid output format"));
    }
}
