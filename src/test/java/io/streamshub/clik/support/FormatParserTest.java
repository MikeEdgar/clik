package io.streamshub.clik.support;

import org.junit.jupiter.api.Test;

import io.streamshub.clik.kafka.model.KafkaRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormatParserTest {

    // ========== Format String Parsing Tests ==========

    @Test
    void testSimplePlaceholders() {
        ParsedFormat format = FormatParser.parse("%k %v");
        KafkaRecord components = format.matchLine("key1 value1");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testAllSimplePlaceholders() {
        ParsedFormat format = FormatParser.parse("%k %v %T %p");
        KafkaRecord components = format.matchLine("mykey myvalue 1735401600000 2");

        assertEquals("mykey", components.keyString(null));
        assertEquals("myvalue", components.valueString(null));
        assertEquals(1735401600000L, components.timestamp());
        assertEquals(2, components.partition());
    }

    @Test
    void testBase64EncodedKey() {
        // "test-key" in base64
        ParsedFormat format = FormatParser.parse("%{base64:k} %v");
        KafkaRecord components = format.matchLine("dGVzdC1rZXk= value1");

        assertEquals("test-key", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testHexEncodedValue() {
        // "Hello" in hex
        ParsedFormat format = FormatParser.parse("%k %{hex:v}");
        KafkaRecord components = format.matchLine("key1 48656c6c6f");

        assertEquals("key1", components.keyString(null));
        assertEquals("Hello", components.valueString(null));
    }

    @Test
    void testNamedHeader() {
        ParsedFormat format = FormatParser.parse("%k %v %{h.content-type}");
        KafkaRecord components = format.matchLine("key1 value1 content-type=application/json");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
        assertEquals("application/json", components.firstHeader("content-type").valueString(null));
    }

    @Test
    void testGenericHeader() {
        ParsedFormat format = FormatParser.parse("%k %v %h");
        KafkaRecord components = format.matchLine("key1 value1 my-header=data");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
        assertEquals("data", components.firstHeader("my-header").valueString(null));
    }

    @Test
    void testBase64EncodedHeader() {
        // "signature" in base64
        ParsedFormat format = FormatParser.parse("%k %v %{base64:h.sig}");
        KafkaRecord components = format.matchLine("key1 value1 sig=U2lnbmF0dXJl");

        assertEquals("key1", components.keyString(null));
        assertEquals("Signature", components.firstHeader("sig").valueString(null));
    }

    @Test
    void testMultipleNamedHeaders() {
        ParsedFormat format = FormatParser.parse("%k %v %{h.type} %{h.version}");
        KafkaRecord components = format.matchLine("key1 value1 type=json version=1.0");

        assertEquals("json", components.firstHeader("type").valueString(null));
        assertEquals("1.0", components.firstHeader("version").valueString(null));
    }

    @Test
    void testEscapedPercent() {
        ParsedFormat format = FormatParser.parse("%k%% %v");
        KafkaRecord components = format.matchLine("key% value");

        assertEquals("key", components.keyString(null));
        assertEquals("value", components.valueString(null));
    }

    @Test
    void testUnicodeEscapeTab() {
        ParsedFormat format = FormatParser.parse("%k\u0009%v");
        KafkaRecord components = format.matchLine("key1\tvalue1");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testUnicodeEscapeSpace() {
        ParsedFormat format = FormatParser.parse("%k\u0020%v");
        KafkaRecord components = format.matchLine("key1 value1");

        assertEquals("key1", components.keyString(null));
        assertEquals("value1", components.valueString(null));
    }

    @Test
    void testMixedEncodings() {
        // hex key, base64 value, plain header
        ParsedFormat format = FormatParser.parse("%{hex:k} %{base64:v} %{h.type}");
        KafkaRecord components = format.matchLine("6b6579 VGVzdA== type=data");

        assertEquals("key", components.keyString(null));
        assertEquals("Test", components.valueString(null));
        assertEquals("data", components.firstHeader("type").valueString(null));
    }

    // ========== Error Cases ==========

    @Test
    void testNullFormatString() {
        assertThrows(IllegalArgumentException.class, () -> FormatParser.parse(null));
    }

    @Test
    void testEmptyFormatString() {
        assertThrows(IllegalArgumentException.class, () -> FormatParser.parse(""));
    }

    @Test
    void testFormatStringWithoutPlaceholders() {
        assertThrows(IllegalArgumentException.class, () -> FormatParser.parse("just literal text"));
    }

    @Test
    void testUnknownSimplePlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%x"));
        assertTrue(ex.getMessage().contains("Unknown placeholder"));
    }

    @Test
    void testUnknownEncoding() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%{unknown:k}"));
        assertTrue(ex.getMessage().contains("Unknown encoding"));
    }

    @Test
    void testUnclosedPlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%{base64:k"));
        assertTrue(ex.getMessage().contains("Unclosed placeholder"));
    }

    @Test
    void testDuplicateNamedHeader() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%{h.type} %{h.type}"));
        assertTrue(ex.getMessage().contains("Duplicate header"));
    }

    @Test
    void testMultipleGenericHeaders() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%h %h"));
        assertTrue(ex.getMessage().contains("Generic header placeholder can only appear once"));
    }

    @Test
    void testEmptyHeaderName() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%{h.}"));
        assertTrue(ex.getMessage().contains("Empty header name"));
    }

    @Test
    void testNamedNonHeaderPlaceholder() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%{k.name}"));
        assertTrue(ex.getMessage().contains("Only header placeholders can have names"));
    }

    @Test
    void testInvalidUnicodeEscape() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%k\\uXXXX%v"));
        assertTrue(ex.getMessage().contains("Invalid unicode escape"));
    }

    @Test
    void testIncompletePercent() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> FormatParser.parse("%k %"));
        assertTrue(ex.getMessage().contains("incomplete placeholder"));
    }

    // ========== Input Line Matching Errors ==========

    @Test
    void testMissingDelimiter() {
        ParsedFormat format = FormatParser.parse("%k %v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("keyvalue")); // missing space delimiter
        assertTrue(ex.getMessage().contains("delimiter") || ex.getMessage().contains("not found"));
    }

    @Test
    void testWrongLiteralAtPosition() {
        ParsedFormat format = FormatParser.parse("%k:%v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("key value")); // has space instead of colon
        assertTrue(ex.getMessage().contains("Expected"));
    }

    @Test
    void testInvalidHeaderFormat() {
        ParsedFormat format = FormatParser.parse("%k %v %h");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("key value noequals")); // header missing =
        assertTrue(ex.getMessage().contains("Invalid header format"));
    }

    @Test
    void testHeaderNameMismatch() {
        ParsedFormat format = FormatParser.parse("%k %v %{h.expected}");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("key value actual=value")); // wrong header name
        assertTrue(ex.getMessage().contains("Expected header 'expected'"));
    }

    @Test
    void testInvalidTimestamp() {
        ParsedFormat format = FormatParser.parse("%k %v %T");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("key value not-a-number"));
        assertTrue(ex.getMessage().contains("Invalid timestamp"));
    }

    @Test
    void testInvalidPartition() {
        ParsedFormat format = FormatParser.parse("%k %v %p");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("key value not-a-number"));
        assertTrue(ex.getMessage().contains("Invalid partition"));
    }

    @Test
    void testInvalidBase64InInput() {
        ParsedFormat format = FormatParser.parse("%{base64:k} %v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("Invalid!@#$ value"));
        assertTrue(ex.getMessage().contains("Invalid base64"));
    }

    @Test
    void testInvalidHexInInput() {
        ParsedFormat format = FormatParser.parse("%{hex:k} %v");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> format.matchLine("gg value")); // invalid hex characters
        assertTrue(ex.getMessage().contains("Invalid hex"));
    }

    // ========== Complex Scenarios ==========

    @Test
    void testValueWithSpaces() {
        ParsedFormat format = FormatParser.parse("%k %v");
        KafkaRecord components = format.matchLine("key value with multiple spaces");

        assertEquals("key", components.keyString(null));
        assertEquals("value with multiple spaces", components.valueString(null));
    }

    @Test
    void testLastPlaceholderConsumesRestOfLine() {
        ParsedFormat format = FormatParser.parse("%k:%v");
        KafkaRecord components = format.matchLine("mykey:value with : colons : inside");

        assertEquals("mykey", components.keyString(null));
        assertEquals("value with : colons : inside", components.valueString(null));
    }

    @Test
    void testEmptyValue() {
        ParsedFormat format = FormatParser.parse("%k %v");
        KafkaRecord components = format.matchLine("key ");

        assertEquals("key", components.keyString(null));
        assertEquals("", components.valueString(null));
    }
}
