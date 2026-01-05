package io.streamshub.clik.support;

import java.util.List;
import java.util.Set;

/**
 * Represents a parsed format string ready to match against input lines.
 * The format string has been tokenized into literals and placeholders,
 * and validated for correctness.
 */
public class ParsedFormat {
    private final List<FormatToken> tokens;
    private final Set<String> namedHeaders;
    private final boolean hasGenericHeader;

    public ParsedFormat(List<FormatToken> tokens, Set<String> namedHeaders, boolean hasGenericHeader) {
        this.tokens = List.copyOf(tokens);
        this.namedHeaders = Set.copyOf(namedHeaders);
        this.hasGenericHeader = hasGenericHeader;
    }

    /**
     * Match an input line against this format and extract message components.
     *
     * @param line The input line to parse
     * @return Parsed message components
     * @throws IllegalArgumentException if the line doesn't match the format
     */
    public MessageComponents matchLine(String line) {
        MessageComponents components = new MessageComponents();
        int linePos = 0;

        for (int i = 0; i < tokens.size(); i++) {
            FormatToken token = tokens.get(i);

            if (token instanceof LiteralToken literal) {
                // Match literal string
                if (!line.startsWith(literal.literal(), linePos)) {
                    throw new IllegalArgumentException(
                        "Expected '" + literal.literal() + "' at position " + linePos);
                }
                linePos += literal.literal().length();
            } else if (token instanceof PlaceholderToken placeholder) {
                // Extract value up to next delimiter
                String value;

                // Find next literal delimiter (or end of line)
                if (i + 1 < tokens.size() && tokens.get(i + 1) instanceof LiteralToken nextLiteral) {
                    int nextPos = line.indexOf(nextLiteral.literal(), linePos);
                    if (nextPos == -1) {
                        throw new IllegalArgumentException(
                            "Expected delimiter '" + nextLiteral.literal() + "' not found");
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

        return components;
    }

    private void processPlaceholder(PlaceholderToken placeholder, String value, MessageComponents components) {
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
        }
    }

    private byte[] decodeWithEncoding(String value, String encoding) {
        if (encoding == null) {
            return Encoding.decodeValue(value);
        } else {
            return Encoding.decodeValue(encoding + ":" + value);
        }
    }
}
