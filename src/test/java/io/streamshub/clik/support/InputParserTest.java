package io.streamshub.clik.support;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.streamshub.clik.kafka.model.KafkaRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InputParserTest {

    // ========== Format String Parsing Tests ==========

    @Test
    void testSimplePlaceholders() {
        InputParser format = InputParser.withFormat("%k %v");
        KafkaRecord components = format.parse("key1 value1");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testAllSimplePlaceholders() {
        InputParser format = InputParser.withFormat("%k %v %T %p");
        KafkaRecord components = format.parse("mykey myvalue 1735401600000 2");

        assertEquals("mykey", components.keyString(null));
        assertEquals("myvalue", components.valueString(null));
        assertEquals(1735401600000L, components.timestamp());
        assertEquals(2, components.partition());
    }

    @Test
    void testBase64EncodedKey() {
        // "test-key" in base64
        InputParser format = InputParser.withFormat("%{base64:k} %v");
        KafkaRecord components = format.parse("dGVzdC1rZXk= value1");

        assertEquals("test-key", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testHexEncodedValue() {
        // "Hello" in hex
        InputParser format = InputParser.withFormat("%k %{hex:v}");
        KafkaRecord components = format.parse("key1 48656c6c6f");

        assertEquals("key1", components.keyString(null));
        assertEquals("Hello", components.valueString(null));
    }

    @Test
    void testNamedHeader() {
        InputParser format = InputParser.withFormat("%k %v %{h.content-type}");
        KafkaRecord components = format.parse("key1 value1 content-type=application/json");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
        assertEquals("application/json", components.firstHeader("content-type").valueString(null));
    }

    @Test
    void testGenericHeader() {
        InputParser format = InputParser.withFormat("%k %v %h");
        KafkaRecord components = format.parse("key1 value1 my-header=data");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
        assertEquals("data", components.firstHeader("my-header").valueString(null));
    }

    @Test
    void testBase64EncodedHeader() {
        // "signature" in base64
        InputParser format = InputParser.withFormat("%k %v %{base64:h.sig}");
        KafkaRecord components = format.parse("key1 value1 sig=U2lnbmF0dXJl");

        assertEquals("key1", components.keyString(null));
        assertEquals("Signature", components.firstHeader("sig").valueString(null));
    }

    @Test
    void testMultipleNamedHeaders() {
        InputParser format = InputParser.withFormat("%k %v %{h.type} %{h.version}");
        KafkaRecord components = format.parse("key1 value1 type=json version=1.0");

        assertEquals("json", components.firstHeader("type").valueString(null));
        assertEquals("1.0", components.firstHeader("version").valueString(null));
    }

    @Test
    void testEscapedPercent() {
        InputParser format = InputParser.withFormat("%k%% %v");
        KafkaRecord components = format.parse("key% value");

        assertEquals("key", components.keyString(null));
        assertEquals("value", components.valueString(null));
    }

    @Test
    void testUnicodeEscapeTab() {
        InputParser format = InputParser.withFormat("%k\u0009%v");
        KafkaRecord components = format.parse("key1\tvalue1");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testUnicodeEscapeSpace() {
        InputParser format = InputParser.withFormat("%k\u0020%v");
        KafkaRecord components = format.parse("key1 value1");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testMixedEncodings() {
        // hex key, base64 value, plain header
        InputParser format = InputParser.withFormat("%{hex:k} %{base64:v} %{h.type}");
        KafkaRecord components = format.parse("6b6579 VGVzdA== type=data");

        assertEquals("key", components.keyString(null));
        assertEquals("Test", components.valueString(null));
        assertEquals("data", components.firstHeader("type").valueString(null));
    }

    @Test
    void testDuplicateNamedHeaders() {
        InputParser format = InputParser.withFormat("%k %v %{h.tag} %{h.tag}");
        KafkaRecord record = format.parse("key1 value1 tag=v1 tag=v2");

        assertEquals("key1", record.keyString(null));
        assertEquals("value1", record.valueString(null));

        // Verify both "tag" headers are present
        List<KafkaRecord.Header> tagHeaders = record.headers("tag");
        assertEquals(2, tagHeaders.size());
        assertEquals("v1", tagHeaders.get(0).valueString(null));
        assertEquals("v2", tagHeaders.get(1).valueString(null));
    }

    @Test
    void testMultipleGenericHeaders() {
        InputParser format = InputParser.withFormat("%k %v %h %h");
        KafkaRecord record = format.parse("key1 value1 type=json version=1.0");

        assertEquals("key1", record.keyString(null));
        assertEquals("value1", record.valueString(null));

        // Verify both headers were parsed with different keys
        assertEquals(2, record.headers().size());
        assertEquals("json", record.firstHeader("type").valueString(null));
        assertEquals("1.0", record.firstHeader("version").valueString(null));
    }

    @Test
    void testMixedDuplicateHeaders() {
        InputParser format = InputParser.withFormat("%k %v %{h.tag} %h %{h.tag}");
        KafkaRecord record = format.parse("key1 value1 tag=v1 type=json tag=v2");

        assertEquals("key1", record.keyString(null));
        assertEquals("value1", record.valueString(null));

        // Verify three headers total
        assertEquals(3, record.headers().size());

        // Verify two "tag" headers
        List<KafkaRecord.Header> tagHeaders = record.headers("tag");
        assertEquals(2, tagHeaders.size());
        assertEquals("v1", tagHeaders.get(0).valueString(null));
        assertEquals("v2", tagHeaders.get(1).valueString(null));

        // Verify one "type" header
        assertEquals("json", record.firstHeader("type").valueString(null));
    }

    @Test
    void testDuplicateHeadersWithEncoding() {
        // "plain" in base64 is "cGxhaW4="
        InputParser format = InputParser.withFormat("%k %v %{h.data} %{base64:h.data}");
        KafkaRecord record = format.parse("key1 value1 data=plain data=cGxhaW4=");

        assertEquals("key1", record.keyString(null));
        assertEquals("value1", record.valueString(null));

        // Verify both "data" headers are present and decoded correctly
        List<KafkaRecord.Header> dataHeaders = record.headers("data");
        assertEquals(2, dataHeaders.size());
        assertEquals("plain", dataHeaders.get(0).valueString(null));
        assertEquals("plain", dataHeaders.get(1).valueString(null)); // base64 decoded
    }

    // ========== Error Cases ==========

    @Test
    void testNullFormatString() {
        assertThrows(IllegalArgumentException.class, () -> InputParser.withFormat(null));
    }

    @Test
    void testEmptyFormatString() {
        assertThrows(IllegalArgumentException.class, () -> InputParser.withFormat(""));
    }

    @Test
    void testFormatStringWithoutPlaceholders() {
        assertThrows(IllegalArgumentException.class, () -> InputParser.withFormat("just literal text"));
    }

    @Test
    void testUnknownSimplePlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%x"));
        assertTrue(ex.getMessage().contains("Unknown placeholder"));
    }

    @Test
    void testUnknownEncoding() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%{unknown:k}"));
        assertTrue(ex.getMessage().contains("Unknown encoding"));
    }

    @Test
    void testUnclosedPlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%{base64:k"));
        assertTrue(ex.getMessage().contains("Unclosed placeholder"));
    }

    @Test
    void testEmptyHeaderName() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%{h.}"));
        assertTrue(ex.getMessage().contains("Empty header name"));
    }

    @Test
    void testNamedNonHeaderPlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%{k.name}"));
        assertTrue(ex.getMessage().contains("Only header placeholders can have names"));
    }

    @Test
    void testInvalidUnicodeEscape() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%k\\uXXXX%v"));
        assertTrue(ex.getMessage().contains("Invalid unicode escape"));
    }

    @Test
    void testIncompletePercent() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> InputParser.withFormat("%k %"));
        assertTrue(ex.getMessage().contains("incomplete placeholder"));
    }

    // ========== Input Line Matching Errors ==========

    @Test
    void testMissingDelimiter() {
        InputParser format = InputParser.withFormat("%k %v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("keyvalue")); // missing space delimiter
        assertTrue(ex.getMessage().contains("delimiter") || ex.getMessage().contains("not found"));
    }

    @Test
    void testWrongLiteralAtPosition() {
        InputParser format = InputParser.withFormat("%k:%v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("key value")); // has space instead of colon
        assertTrue(ex.getMessage().contains("Expected"));
    }

    @Test
    void testInvalidHeaderFormat() {
        InputParser format = InputParser.withFormat("%k %v %h");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("key value noequals")); // header missing =
        assertTrue(ex.getMessage().contains("Invalid header format"));
    }

    @Test
    void testHeaderNameMismatch() {
        InputParser format = InputParser.withFormat("%k %v %{h.expected}");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("key value actual=value")); // wrong header name
        assertTrue(ex.getMessage().contains("Expected header 'expected'"));
    }

    @Test
    void testInvalidTimestamp() {
        InputParser format = InputParser.withFormat("%k %v %T");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("key value not-a-number"));
        assertTrue(ex.getMessage().contains("Invalid timestamp"));
    }

    @Test
    void testInvalidPartition() {
        InputParser format = InputParser.withFormat("%k %v %p");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("key value not-a-number"));
        assertTrue(ex.getMessage().contains("Invalid partition"));
    }

    @Test
    void testInvalidBase64InInput() {
        InputParser format = InputParser.withFormat("%{base64:k} %v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("Invalid!@#$ value"));
        assertTrue(ex.getMessage().contains("Invalid base64"));
    }

    @Test
    void testInvalidHexInInput() {
        InputParser format = InputParser.withFormat("%{hex:k} %v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.parse("gg value")); // invalid hex characters
        assertTrue(ex.getMessage().contains("Invalid hex"));
    }

    // ========== Complex Scenarios ==========

    @Test
    void testValueWithSpaces() {
        InputParser format = InputParser.withFormat("%k %v");
        KafkaRecord components = format.parse("key value with multiple spaces");

        assertEquals("key", components.keyString(null));
        assertEquals("value with multiple spaces", components.valueString(null));
    }

    @Test
    void testLastPlaceholderConsumesRestOfLine() {
        InputParser format = InputParser.withFormat("%k:%v");
        KafkaRecord components = format.parse("mykey:value with : colons : inside");

        assertEquals("mykey", components.keyString(null));
        assertEquals("value with : colons : inside", components.valueString(null));
    }

    @Test
    void testEmptyValue() {
        InputParser format = InputParser.withFormat("%k %v");
        KafkaRecord components = format.parse("key ");

        assertEquals("key", components.keyString(null));
        assertEquals("", components.valueString(null));
    }
}
