package io.streamshub.clik.support;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncodingTest {

    // ========== Parameterized Valid String Decoding Tests ==========

    @ParameterizedTest(name = "[{index}] input=''{0}'' should decode to ''{1}''")
    @CsvSource({
        // Base64 tests
        "base64:SGVsbG8gV29ybGQ=, Hello World",
        "base64:SGVsbG8, Hello",
        "base64:dGVzdC1rZXk=, test-key",
        "base64:U2lnbmF0dXJl, Signature",
        // Hex tests - lowercase
        "hex:48656c6c6f, Hello",
        "hex:6b6579, key",
        // Hex tests - uppercase
        "hex:48656C6C6F, Hello",
        // Hex tests - mixed case
        "hex:48656c6C6f, Hello",
        // Hex with whitespace
        "'hex:48 65 6c 6c 6f', Hello",
        // Plain text (no prefix)
        "Hello World, Hello World",
        "simple-key, simple-key",
        "simple value, simple value",
        // Plain text with colon but no recognized prefix
        "unknown:value, unknown:value",
        // Prefix should be lowercase only - uppercase treated as plain string
        "BASE64:SGVsbG8=, BASE64:SGVsbG8=",
        "HEX:48656c6c6f, HEX:48656c6c6f",
    })
    void testValidStringDecoding(String input, String expectedOutput) {
        byte[] result = Encoding.decodeValue(input);
        assertEquals(expectedOutput, new String(result, StandardCharsets.UTF_8));
    }

    // ========== Empty String Tests ==========

    @ParameterizedTest(name = "[{index}] ''{0}'' should decode to empty byte array")
    @ValueSource(strings = {"base64:", "hex:", ""})
    void testEmptyStringDecoding(String input) {
        byte[] result = Encoding.decodeValue(input);
        assertArrayEquals(new byte[0], result);
    }

    // ========== Binary Data Tests ==========

    @Test
    void testBase64BinaryData() {
        // Binary data that's not valid UTF-8
        byte[] binaryData = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        String base64 = "base64:" + java.util.Base64.getEncoder().encodeToString(binaryData);
        byte[] result = Encoding.decodeValue(base64);
        assertArrayEquals(binaryData, result);
    }

    @Test
    void testHexBinaryData() {
        String input = "hex:deadbeef";
        byte[] result = Encoding.decodeValue(input);
        byte[] expected = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        assertArrayEquals(expected, result);
    }

    // ========== Unicode and Special Characters ==========

    @Test
    void testPlainStringWithUnicode() {
        String input = "Hello 世界 🌍";
        byte[] result = Encoding.decodeValue(input);
        assertEquals(input, new String(result, StandardCharsets.UTF_8));
    }

    @Test
    void testPlainStringWithNewlines() {
        String input = "line1\nline2\nline3";
        byte[] result = Encoding.decodeValue(input);
        assertEquals(input, new String(result, StandardCharsets.UTF_8));
    }

    // ========== Null Test ==========

    @Test
    void testNullValue() {
        byte[] result = Encoding.decodeValue(null);
        assertNull(result);
    }

    // ========== Error Cases ==========

    @ParameterizedTest(name = "[{index}] ''{0}'' should throw with message containing ''{1}''")
    @CsvSource({
        "base64:Invalid!@#$%, Invalid base64 encoding",
        "hex:abc, odd number of characters",
        "hex:gg, Invalid hex encoding at position 0"
    })
    void testInvalidEncoding(String input, String expectedErrorMessageFragment) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> Encoding.decodeValue(input)
        );
        assertTrue(ex.getMessage().contains(expectedErrorMessageFragment),
            "Expected error message to contain '" + expectedErrorMessageFragment +
            "' but was: " + ex.getMessage());
    }
}
