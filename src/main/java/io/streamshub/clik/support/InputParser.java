package io.streamshub.clik.support;

import java.util.List;

import io.streamshub.clik.kafka.model.KafkaRecord;

/**
 * Parser for format strings used in the --input option.
 * Delegates format string parsing to FormatStringParser.
 *
 * Format strings define how to parse structured input lines into Kafka message components.
 *
 * Supported placeholders:
 * - Simple: %k (key), %v (value), %h (header), %T (timestamp), %p (partition), %% (literal %)
 * - Parameterized: %{base64:k}, %{hex:v}, %{h.name}, %{base64:h.signature}
 * - Unicode escapes: {@literal \}uXXXX for any unicode character
 *
 * Examples:
 * - "%k %v" - key and value separated by space
 * - "%{base64:k}\u0009%v" - base64 key and value separated by tab
 * - "%k %v %{h.type}" - key, value, and a named header
 */
public class InputParser {
    private final List<FormatToken> tokens;

    private InputParser(List<FormatToken> tokens) {
        this.tokens = List.copyOf(tokens);
    }

    /**
     * Parse a format string into an InputParser ready to match input lines.
     *
     * @param format The format string
     * @return InputParser ready to parse input lines
     * @throws IllegalArgumentException if the format string is invalid
     */
    public static InputParser withFormat(String format) {
        List<FormatToken> tokens = FormatStringParser.parseFormatString(format);
        return new InputParser(tokens);
    }

    /**
     * Match an input line against this format and extract message components.
     *
     * @param line The input line to parse
     * @return Parsed message components
     * @throws IllegalArgumentException if the line doesn't match the format
     */
    public KafkaRecord parse(String line) {
        KafkaRecord.Builder components = KafkaRecord.builder();
        int linePos = 0;

        for (int i = 0; i < tokens.size(); i++) {
            FormatToken token = tokens.get(i);

            if (token instanceof LiteralToken(String literal)) {
                // Match literal string
                if (!line.startsWith(literal, linePos)) {
                    throw new IllegalArgumentException(
                        "Expected '" + literal + "' at position " + linePos);
                }
                linePos += literal.length();
            } else if (token instanceof PlaceholderToken placeholder) {
                // Extract value up to next delimiter
                String value;

                // Find next literal delimiter (or end of line)
                if (i + 1 < tokens.size() && tokens.get(i + 1) instanceof LiteralToken(String nextLiteral)) {
                    int nextPos = line.indexOf(nextLiteral, linePos);
                    if (nextPos == -1) {
                        throw new IllegalArgumentException(
                            "Expected delimiter '" + nextLiteral + "' not found");
                    }
                    value = line.substring(linePos, nextPos);
                    linePos = nextPos;
                } else {
                    // Last placeholder, consume rest of line
                    value = line.substring(linePos);
                    linePos = line.length();
                }

                // Process the placeholder
                processPlaceholder(placeholder, value, components);
            }
        }

        return components.build();
    }

    private void processPlaceholder(PlaceholderToken placeholder, String value, KafkaRecord.Builder components) {
        switch (placeholder.type()) {
            case KEY -> components.setKey(decodeWithEncoding(value, placeholder.encoding()));

            case VALUE -> components.setValue(decodeWithEncoding(value, placeholder.encoding()));

            case HEADER -> {
                // Parse header as key=value
                int eqIndex = value.indexOf('=');
                if (eqIndex <= 0) {
                    throw new IllegalArgumentException(
                        "Invalid header format: " + value + ". Expected key=value");
                }

                String headerKey = value.substring(0, eqIndex);
                String headerValue = value.substring(eqIndex + 1);

                // If this is a named placeholder, verify the key matches
                if (placeholder.name() != null && !headerKey.equals(placeholder.name())) {
                    throw new IllegalArgumentException(
                        "Expected header '" + placeholder.name() + "' but got '" + headerKey + "'");
                }

                // Decode header value (not key)
                byte[] headerBytes = decodeWithEncoding(headerValue, placeholder.encoding());
                components.addHeader(headerKey, headerBytes);
            }

            case TIMESTAMP -> {
                try {
                    components.setTimestamp(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid timestamp: " + value, e);
                }
            }

            case PARTITION -> {
                try {
                    components.setPartition(Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid partition: " + value, e);
                }
            }

            case OFFSET -> {
                // Offset is ignored in the input file/stream.
                break;
            }
        }
    }

    private byte[] decodeWithEncoding(String value, String encoding) {
        return Encoding.decodeValue(value, encoding);
    }
}
