package io.streamshub.clik.support;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Encoding {
    private static final String BASE64 = "base64";
    private static final String HEX = "hex";
    private static final String BASE64_PREFIX = BASE64 + ':';
    private static final String HEX_PREFIX = HEX + ':';

    private Encoding() {
        // No instances
    }

    /**
     * Decode a string that may have a prefix indicating encoding.
     * Supported prefixes: "base64:", "hex:"
     * If no prefix, treats as UTF-8 string.
     *
     * @param value The value to decode (may have prefix)
     * @return Decoded bytes
     * @throws IllegalArgumentException if encoding is invalid
     */
    public static byte[] decodeValue(String value) {
        if (value == null) {
            return null; // NOSONAR
        }

        if (value.startsWith(BASE64_PREFIX)) {
            return decodeValue(value.substring(BASE64_PREFIX.length()), BASE64);
        } else if (value.startsWith(HEX_PREFIX)) {
            return decodeValue(value.substring(HEX_PREFIX.length()), HEX);
        } else {
            // Default: treat as UTF-8 string
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Decode a string with the indicated encoding.
     * Supported encodings: "base64", "hex"
     * If unknown encoding, treats as UTF-8 string.
     *
     * @param value The value to decode (without prefix)
     * @param encoding The encoding used for the value
     * @return Decoded bytes
     * @throws IllegalArgumentException if encoding is invalid
     */
    public static byte[] decodeValue(String value, String encoding) {
        if (value == null) {
            return null; // NOSONAR
        }

        if (BASE64.equals(encoding)) {
            try {
                return Base64.getDecoder().decode(value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Invalid base64 encoding: " + e.getMessage(), e);
            }
        } else if (HEX.equals(encoding)) {
            return decodeHex(value);
        } else {
            // Default: treat as UTF-8 string
            return value.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Decode a hexadecimal string to bytes.
     * Accepts both uppercase and lowercase hex digits.
     *
     * @param hex The hex string (without "hex:" prefix)
     * @return Decoded bytes
     * @throws IllegalArgumentException if hex string is invalid
     */
    private static byte[] decodeHex(String hex) {
        // Remove any whitespace
        hex = hex.replaceAll("\\s", "");

        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException(
                "Invalid hex encoding: odd number of characters");
        }

        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            int index = i * 2;
            try {
                bytes[i] = (byte) Integer.parseInt(
                    hex.substring(index, index + 2), 16);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid hex encoding at position " + index + ": " +
                    hex.substring(index, Math.min(index + 2, hex.length())), e);
            }
        }
        return bytes;
    }
}
