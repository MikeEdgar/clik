package io.streamshub.clik.support;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.streamshub.clik.kafka.model.KafkaRecord;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toCollection;

/**
 * Formats Kafka records according to a format string template.
 * Delegates format string parsing to FormatStringParser.
 */
public class OutputFormatter {
    private static final Deque<byte[]> EMPTY = new ArrayDeque<>(0);

    private final List<FormatToken> tokens;

    private OutputFormatter(List<FormatToken> tokens) {
        this.tokens = tokens;
    }

    /**
     * Parse a format string into an OutputFormatter.
     *
     * @param format The format string (e.g., "%k %v %T")
     * @return OutputFormatter ready to format records
     * @throws IllegalArgumentException if format string is invalid
     */
    public static OutputFormatter withFormat(String format) {
        List<FormatToken> tokens = FormatStringParser.parseFormatString(format);
        return new OutputFormatter(tokens);
    }

    /**
     * Format a Kafka record according to the parsed format string.
     *
     * @param record The record to format
     * @return Formatted string
     */
    public String format(KafkaRecord record) {
        StringBuilder result = new StringBuilder();
        Map<String, Deque<byte[]>> headers = record.headers()
                .stream()
                .collect(groupingBy(
                        KafkaRecord.Header::key,
                        mapping(
                            KafkaRecord.Header::valueBytes,
                            toCollection(ArrayDeque::new)
                        )
                ));

        for (FormatToken token : tokens) {
            if (token instanceof LiteralToken(String literal)) {
                result.append(literal);
            } else if (token instanceof PlaceholderToken placeholder) {
                String value = formatPlaceholder(record, headers, placeholder);
                result.append(value != null ? value : "");
            }
        }

        return result.toString();
    }

    private String formatPlaceholder(KafkaRecord record, Map<String, Deque<byte[]>> headers, PlaceholderToken placeholder) {
        PlaceholderType type = placeholder.type();
        String encoding = placeholder.encoding();
        String name = placeholder.name();

        return switch (type) {
            case KEY -> encodeValue(record.keyBytes(), encoding);
            case VALUE -> encodeValue(record.valueBytes(), encoding);
            case PARTITION -> record.partition() != null ? String.valueOf(record.partition()) : "";
            case OFFSET -> record.offset() != null ? String.valueOf(record.offset()) : "";
            case TIMESTAMP -> record.timestamp() != null ? String.valueOf(record.timestamp()) : "";
            case HEADER -> {
                if (name != null) {
                    // Named header: %{h.name} or %{base64:h.name}
                    var remainingValues = headers.getOrDefault(name, EMPTY);
                    var result = remainingValues.stream()
                        .map(value -> name + "=" + encodeValue(value, encoding))
                        .collect(Collectors.joining(" "));
                    remainingValues.clear();
                    yield result;
                } else {
                    // Generic header: %h - output all remaining headers as key=value pairs
                    yield headers.entrySet().stream()
                        .filter(h -> !h.getValue().isEmpty()) // skip headers already output
                        .flatMap(h -> {
                            List<byte[]> values = new ArrayList<>(h.getValue());
                            h.getValue().clear(); // clear to indicate already output
                            return values.stream().map(value -> Map.entry(h.getKey(), value));
                        })
                        .map(h -> h.getKey() + "=" + encodeValue(h.getValue(), encoding))
                        .collect(Collectors.joining(" "));
                }
            }
        };
    }

    /**
     * Encode bytes to string based on encoding type.
     * For output, encoding means ENCODE (opposite of input parsing which decodes).
     *
     * @param bytes The bytes to encode
     * @param encoding The encoding type: "base64", "hex", or null for UTF-8
     * @return Encoded string
     */
    private String encodeValue(byte[] bytes, String encoding) {
        if (bytes == null) {
            return null;
        }

        if (encoding == null) {
            // Default: UTF-8 string
            return new String(bytes, StandardCharsets.UTF_8);
        }

        return switch (encoding) {
            case "base64" -> Base64.getEncoder().encodeToString(bytes);
            case "hex" -> encodeHex(bytes);
            default -> new String(bytes, StandardCharsets.UTF_8);
        };
    }

    /**
     * Encode bytes to hexadecimal string.
     *
     * @param bytes The bytes to encode
     * @return Hex string (lowercase)
     */
    private String encodeHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
