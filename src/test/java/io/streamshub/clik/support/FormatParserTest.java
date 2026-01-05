package io.streamshub.clik.support;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FormatParserTest {

    // ========== Format String Parsing Tests ==========

    @Test
    void testSimplePlaceholders() {
        ParsedFormat format = FormatParser.parse("%k %v");
        MessageComponents components = format.matchLine("key1 value1");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value1", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testAllSimplePlaceholders() {
        ParsedFormat format = FormatParser.parse("%k %v %T %p");
        MessageComponents components = format.matchLine("mykey myvalue 1735401600000 2");

        assertEquals("mykey", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("myvalue", new String(components.getValue(), StandardCharsets.UTF_8));
        assertEquals(1735401600000L, components.getTimestamp());
        assertEquals(2, components.getPartition());
    }

    @Test
    void testBase64EncodedKey() {
        // "test-key" in base64
        ParsedFormat format = FormatParser.parse("%{base64:k} %v");
        MessageComponents components = format.matchLine("dGVzdC1rZXk= value1");

        assertEquals("test-key", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value1", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testHexEncodedValue() {
        // "Hello" in hex
        ParsedFormat format = FormatParser.parse("%k %{hex:v}");
        MessageComponents components = format.matchLine("key1 48656c6c6f");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("Hello", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testNamedHeader() {
        ParsedFormat format = FormatParser.parse("%k %v %{h.content-type}");
        MessageComponents components = format.matchLine("key1 value1 content-type=application/json");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value1", new String(components.getValue(), StandardCharsets.UTF_8));
        assertEquals("application/json",
            new String(components.getHeaders().get("content-type"), StandardCharsets.UTF_8));
    }

    @Test
    void testGenericHeader() {
        ParsedFormat format = FormatParser.parse("%k %v %h");
        MessageComponents components = format.matchLine("key1 value1 my-header=data");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value1", new String(components.getValue(), StandardCharsets.UTF_8));
        assertEquals("data",
            new String(components.getHeaders().get("my-header"), StandardCharsets.UTF_8));
    }

    @Test
    void testBase64EncodedHeader() {
        // "signature" in base64
        ParsedFormat format = FormatParser.parse("%k %v %{base64:h.sig}");
        MessageComponents components = format.matchLine("key1 value1 sig=U2lnbmF0dXJl");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("Signature",
            new String(components.getHeaders().get("sig"), StandardCharsets.UTF_8));
    }

    @Test
    void testMultipleNamedHeaders() {
        ParsedFormat format = FormatParser.parse("%k %v %{h.type} %{h.version}");
        MessageComponents components = format.matchLine("key1 value1 type=json version=1.0");

        Map<String, byte[]> headers = components.getHeaders();
        assertEquals("json", new String(headers.get("type"), StandardCharsets.UTF_8));
        assertEquals("1.0", new String(headers.get("version"), StandardCharsets.UTF_8));
    }

    @Test
    void testEscapedPercent() {
        ParsedFormat format = FormatParser.parse("%k%% %v");
        MessageComponents components = format.matchLine("key% value");

        assertEquals("key", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testUnicodeEscapeTab() {
        ParsedFormat format = FormatParser.parse("%k\u0009%v");
        MessageComponents components = format.matchLine("key1\tvalue1");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value1", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testUnicodeEscapeSpace() {
        ParsedFormat format = FormatParser.parse("%k\u0020%v");
        MessageComponents components = format.matchLine("key1 value1");

        assertEquals("key1", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value1", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testMixedEncodings() {
        // hex key, base64 value, plain header
        ParsedFormat format = FormatParser.parse("%{hex:k} %{base64:v} %{h.type}");
        MessageComponents components = format.matchLine("6b6579 VGVzdA== type=data");

        assertEquals("key", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("Test", new String(components.getValue(), StandardCharsets.UTF_8));
        assertEquals("data", new String(components.getHeaders().get("type"), StandardCharsets.UTF_8));
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
        MessageComponents components = format.matchLine("key value with multiple spaces");

        assertEquals("key", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value with multiple spaces", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testLastPlaceholderConsumesRestOfLine() {
        ParsedFormat format = FormatParser.parse("%k:%v");
        MessageComponents components = format.matchLine("mykey:value with : colons : inside");

        assertEquals("mykey", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("value with : colons : inside", new String(components.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testEmptyValue() {
        ParsedFormat format = FormatParser.parse("%k %v");
        MessageComponents components = format.matchLine("key ");

        assertEquals("key", new String(components.getKey(), StandardCharsets.UTF_8));
        assertEquals("", new String(components.getValue(), StandardCharsets.UTF_8));
    }
}
