package io.streamshub.clik.support;

import io.streamshub.clik.kafka.model.KafkaRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class OutputFormatterTest {

    // ========== Helper Methods ==========

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static KafkaRecord buildRecord(String key, String value) {
        KafkaRecord.Builder builder = KafkaRecord.builder();
        if (key != null) builder.setKey(bytes(key));
        if (value != null) builder.setValue(bytes(value));
        return builder.build();
    }

    // ========== Simple Placeholder Tests ==========

    @Test
    void testSimpleKeyValue() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v");
        KafkaRecord rec = buildRecord("test-key", "test-value");

        String output = formatter.format(rec);
        assertEquals("test-key test-value", output);
    }

    @Test
    void testAllSimplePlaceholders() {
        OutputFormatter formatter = OutputFormatter.withFormat("%p %o %k %v %T");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setPartition(2);
        builder.setOffset(100L);
        builder.setKey(bytes("mykey"));
        builder.setValue(bytes("myvalue"));
        builder.setTimestamp(1735401600000L);
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("2 100 mykey myvalue 1735401600000", output);
    }

    @Test
    void testKeyOnly() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k");
        KafkaRecord rec = buildRecord("key1", null);

        String output = formatter.format(rec);
        assertEquals("key1", output);
    }

    @Test
    void testValueOnly() {
        OutputFormatter formatter = OutputFormatter.withFormat("%v");
        KafkaRecord rec = buildRecord(null, "value1");

        String output = formatter.format(rec);
        assertEquals("value1", output);
    }

    @Test
    void testPartitionAndOffset() {
        OutputFormatter formatter = OutputFormatter.withFormat("%p:%o");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setPartition(5);
        builder.setOffset(999L);
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("5:999", output);
    }

    // ========== Encoding Tests ==========

    @Test
    void testBase64EncodedKey() {
        OutputFormatter formatter = OutputFormatter.withFormat("%{base64:k} %v");
        KafkaRecord rec = buildRecord("test-key", "value");

        String output = formatter.format(rec);
        String expectedKey = Base64.getEncoder().encodeToString(bytes("test-key"));
        assertEquals(expectedKey + " value", output);
    }

    @Test
    void testHexEncodedValue() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %{hex:v}");
        KafkaRecord rec = buildRecord("key1", "Hello");

        String output = formatter.format(rec);
        assertEquals("key1 48656c6c6f", output);
    }

    @Test
    void testMixedEncodings() {
        OutputFormatter formatter = OutputFormatter.withFormat("%{hex:k} %{base64:v}");
        KafkaRecord rec = buildRecord("key", "Test");

        String output = formatter.format(rec);
        String expectedValue = Base64.getEncoder().encodeToString(bytes("Test"));
        assertEquals("6b6579 " + expectedValue, output);
    }

    // ========== Header Tests ==========

    @Test
    void testNamedHeader() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v %{h[content-type]}");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key1"));
        builder.setValue(bytes("value1"));
        builder.addHeader("content-type", bytes("application/json"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("key1 value1 content-type=application/json", output);
    }

    @Test
    void testGenericHeader() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v %h");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key1"));
        builder.setValue(bytes("value1"));
        builder.addHeader("type", bytes("test"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("key1 value1 type=test", output);
    }

    @Test
    void testMultipleHeaders() {
        OutputFormatter formatter = OutputFormatter.withFormat("%v %h");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setValue(bytes("data"));
        builder.addHeader("type", bytes("json"));
        builder.addHeader("version", bytes("1.0"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("data type=json version=1.0", output);
    }

    @Test
    void testDuplicateNamedHeaders() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v %{h[tag]}");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key1"));
        builder.setValue(bytes("value1"));
        builder.addHeader("tag", bytes("v1"));
        builder.addHeader("tag", bytes("v2"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        // firstHeader() returns the first one
        assertEquals("key1 value1 tag=v1 tag=v2", output);
    }

    @Test
    void testBase64EncodedHeader() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v %{base64:h[sig]}");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key1"));
        builder.setValue(bytes("value1"));
        builder.addHeader("sig", bytes("Signature"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        String expectedSig = Base64.getEncoder().encodeToString(bytes("Signature"));
        assertEquals("key1 value1 sig=" + expectedSig, output);
    }

    @Test
    void testHexEncodedHeader() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %{hex:h[checksum]}");
        byte[] checksumBytes = new byte[]{(byte) 0xA1, (byte) 0xB2, (byte) 0xC3, (byte) 0xD4};
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key1"));
        builder.addHeader("checksum", checksumBytes);
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("key1 checksum=a1b2c3d4", output);
    }

    @Test
    void testGenericHeaderWithEncoding() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %{base64:h}");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key1"));
        builder.addHeader("data", bytes("test"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        String expectedValue = Base64.getEncoder().encodeToString(bytes("test"));
        assertEquals("key1 data=" + expectedValue, output);
    }

    // ========== Null Handling Tests ==========

    @Test
    void testNullKey() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v");
        KafkaRecord rec = buildRecord(null, "value1");

        String output = formatter.format(rec);
        assertEquals(" value1", output);
    }

    @Test
    void testNullValue() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v");
        KafkaRecord rec = buildRecord("key1", null);

        String output = formatter.format(rec);
        assertEquals("key1 ", output);
    }

    @Test
    void testNullPartition() {
        OutputFormatter formatter = OutputFormatter.withFormat("%p %o %v");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setOffset(100L);
        builder.setValue(bytes("value"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals(" 100 value", output);
    }

    @Test
    void testNullTimestamp() {
        OutputFormatter formatter = OutputFormatter.withFormat("%T %v");
        KafkaRecord rec = buildRecord(null, "value");

        String output = formatter.format(rec);
        assertEquals(" value", output);
    }

    @Test
    void testMissingHeader() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v %{h[missing]}");
        KafkaRecord rec = buildRecord("key1", "value1");

        String output = formatter.format(rec);
        assertEquals("key1 value1 ", output);
    }

    @Test
    void testNoHeaders() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k %v %h");
        KafkaRecord rec = buildRecord("key1", "value1");

        String output = formatter.format(rec);
        assertEquals("key1 value1 ", output);
    }

    // ========== Escaping Tests ==========

    @Test
    void testEscapedPercent() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k%% %v");
        KafkaRecord rec = buildRecord("key", "value");

        String output = formatter.format(rec);
        assertEquals("key% value", output);
    }

    @Test
    void testUnicodeEscapeTab() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k\\u0009%v");
        KafkaRecord rec = buildRecord("key1", "value1");

        String output = formatter.format(rec);
        assertEquals("key1\tvalue1", output);
    }

    @Test
    void testUnicodeEscapeNewline() {
        OutputFormatter formatter = OutputFormatter.withFormat("%k\\u000a%v");
        KafkaRecord rec = buildRecord("key1", "value1");

        String output = formatter.format(rec);
        assertEquals("key1\nvalue1", output);
    }

    // ========== Complex Scenarios ==========

    @Test
    void testComplexFormat() {
        OutputFormatter formatter = OutputFormatter.withFormat("[%T] %p:%o %k=%v %{h[named-header]} headers=%h");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setTimestamp(1735401600000L);
        builder.setPartition(3);
        builder.setOffset(50L);
        builder.setKey(bytes("mykey"));
        builder.setValue(bytes("myvalue"));
        builder.addHeader("named-header", bytes("test"));
        builder.addHeader("type", bytes("test"));
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("[1735401600000] 3:50 mykey=myvalue named-header=test headers=type=test", output);
    }

    @Test
    void testJsonLikeFormat() {
        OutputFormatter formatter = OutputFormatter.withFormat("{\"key\":\"%k\",\"value\":\"%v\",\"partition\":%p}");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("test"));
        builder.setValue(bytes("data"));
        builder.setPartition(1);
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("{\"key\":\"test\",\"value\":\"data\",\"partition\":1}", output);
    }

    @Test
    void testCsvFormat() {
        OutputFormatter formatter = OutputFormatter.withFormat("\"%k\",\"%v\",%p,%o,%T");
        KafkaRecord.Builder builder = KafkaRecord.builder();
        builder.setKey(bytes("key"));
        builder.setValue(bytes("value"));
        builder.setPartition(0);
        builder.setOffset(10L);
        builder.setTimestamp(1234567890L);
        KafkaRecord rec = builder.build();

        String output = formatter.format(rec);
        assertEquals("\"key\",\"value\",0,10,1234567890", output);
    }

    // ========== Error Cases ==========

    @Test
    void testNullFormatString() {
        assertThrows(IllegalArgumentException.class, () -> OutputFormatter.withFormat(null));
    }

    @Test
    void testEmptyFormatString() {
        assertThrows(IllegalArgumentException.class, () -> OutputFormatter.withFormat(""));
    }

    @Test
    void testFormatStringWithoutPlaceholders() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("just literal text"));
        assertTrue(ex.getMessage().contains("must contain at least one placeholder"));
    }

    @Test
    void testUnknownSimplePlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%x"));
        assertTrue(ex.getMessage().contains("Unknown placeholder"));
    }

    @Test
    void testUnknownEncoding() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%{unknown:k}"));
        assertTrue(ex.getMessage().contains("Unknown encoding"));
    }

    @Test
    void testUnclosedPlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%{base64:k"));
        assertTrue(ex.getMessage().contains("Unclosed placeholder"));
    }

    @Test
    void testEmptyHeaderName() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%{h[]}"));
        assertTrue(ex.getMessage().contains("Empty header name"));
    }

    @Test
    void testNamedNonHeaderPlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%{k.name}"));
        assertTrue(ex.getMessage().contains("Only header placeholders can have names"));
    }

    @Test
    void testInvalidUnicodeEscape() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%k\\uXXXX%v"));
        assertTrue(ex.getMessage().contains("Invalid unicode escape"));
    }

    @Test
    void testIncompletePercent() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> OutputFormatter.withFormat("%k %"));
        assertTrue(ex.getMessage().contains("incomplete placeholder"));
    }
}
