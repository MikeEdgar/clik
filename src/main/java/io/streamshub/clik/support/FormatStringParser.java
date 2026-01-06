package io.streamshub.clik.support;

import java.util.ArrayList;
import java.util.List;

/**
 * Package-private base class for parsing format strings.
 * Contains shared logic for parsing format strings used by both InputParser and OutputFormatter.
 */
class FormatStringParser {

    private FormatStringParser() {
        // Static utility class
    }

    /**
     * Parse a format string into a list of tokens.
     *
     * @param format The format string to parse
     * @return List of format tokens
     * @throws IllegalArgumentException if format string is invalid
     */
    static List<FormatToken> parseFormatString(String format) {
        if (format == null || format.isEmpty()) {
            throw new IllegalArgumentException("Format string cannot be null or empty");
        }

        // Process unicode escapes first
        format = processUnicodeEscapes(format);

        List<FormatToken> tokens = new ArrayList<>();
        boolean hasPlaceholder = false;
        int i = 0;

        while (i < format.length()) {
            char c = format.charAt(i);

            if (c == '%') {
                if (i + 1 >= format.length()) {
                    throw new IllegalArgumentException(
                        "Format string ends with incomplete placeholder at position " + i);
                }

                char next = format.charAt(i + 1);

                if (next == '%') {
                    // Escaped percent
                    tokens.add(new LiteralToken("%"));
                    i += 2;
                } else if (next == '{') {
                    // Parameterized placeholder: %{encoding:type} or %{type.name}
                    PlaceholderToken token = parseParameterizedPlaceholder(format, i);
                    tokens.add(token);
                    hasPlaceholder = true;
                    // Skip past the closing brace
                    i = format.indexOf('}', i) + 1;
                } else {
                    // Simple placeholder: %k, %v, etc.
                    PlaceholderToken token = parseSimplePlaceholder(next);
                    tokens.add(token);
                    hasPlaceholder = true;
                    i += 2;
                }
            } else {
                // Literal character - collect until next placeholder
                StringBuilder literal = new StringBuilder();
                while (i < format.length() && format.charAt(i) != '%') {
                    literal.append(format.charAt(i));
                    i++;
                }
                tokens.add(new LiteralToken(literal.toString()));
            }
        }

        if (!hasPlaceholder) {
            throw new IllegalArgumentException(
                "Format string must contain at least one placeholder (e.g., %k, %v)");
        }

        return tokens;
    }

    /**
     * Process unicode escape sequences (\\uXXXX) in the format string.
     */
    static String processUnicodeEscapes(String format) {
        StringBuilder result = new StringBuilder();
        int i = 0;

        while (i < format.length()) {
            if (i + 5 < format.length() && format.charAt(i) == '\\' && format.charAt(i + 1) == 'u') {
                // Found potential unicode escape
                String unicodeHex = format.substring(i + 2, i + 6);
                try {
                    int codePoint = Integer.parseInt(unicodeHex, 16);
                    result.append((char) codePoint);
                    i += 6;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Invalid unicode escape at position " + i + ": \\u" + unicodeHex, e);
                }
            } else {
                result.append(format.charAt(i));
                i++;
            }
        }

        return result.toString();
    }

    /**
     * Parse a simple placeholder like %k, %v, %T, %p, %h, %o.
     */
    static PlaceholderToken parseSimplePlaceholder(char type) {
        return switch (type) {
            case 'k' -> new PlaceholderToken(PlaceholderType.KEY, null, null);
            case 'v' -> new PlaceholderToken(PlaceholderType.VALUE, null, null);
            case 'T' -> new PlaceholderToken(PlaceholderType.TIMESTAMP, null, null);
            case 'p' -> new PlaceholderToken(PlaceholderType.PARTITION, null, null);
            case 'o' -> new PlaceholderToken(PlaceholderType.OFFSET, null, null);
            case 'h' -> new PlaceholderToken(PlaceholderType.HEADER, null, null);
            default -> throw new IllegalArgumentException(
                "Unknown placeholder: %" + type + ". " +
                "Valid placeholders: %k (key), %v (value), %T (timestamp), %p (partition), %o (offset), %h (header)");
        };
    }

    /**
     * Parse a parameterized placeholder like %{base64:k}, %{hex:v}, or %{h.name}.
     */
    static PlaceholderToken parseParameterizedPlaceholder(String format, int startPos) {
        int endPos = format.indexOf('}', startPos);
        if (endPos == -1) {
            throw new IllegalArgumentException(
                "Unclosed placeholder at position " + startPos + ": " + format.substring(startPos));
        }

        // Extract content between %{ and }
        String content = format.substring(startPos + 2, endPos);

        // Check for encoding prefix (base64: or hex:)
        String encoding = null;
        String remainder = content;

        if (content.contains(":")) {
            int colonPos = content.indexOf(':');
            String potentialEncoding = content.substring(0, colonPos);
            if (potentialEncoding.equals("base64") || potentialEncoding.equals("hex")) {
                encoding = potentialEncoding;
                remainder = content.substring(colonPos + 1);
            } else {
                // Check if this looks like an encoding attempt but is invalid
                String afterColon = content.substring(colonPos + 1);
                if (!afterColon.startsWith("h.")) {
                    // This is likely an invalid encoding like %{unknown:k}
                    throw new IllegalArgumentException(
                        "Unknown encoding: " + potentialEncoding + " at position " + startPos);
                }
                // Otherwise it might be part of a header name, continue processing
            }
        }

        // Parse the placeholder type and optional name
        if (remainder.startsWith("h.")) {
            // Named header: %{h.name} or %{base64:h.name}
            String headerName = remainder.substring(2);
            if (headerName.isEmpty()) {
                throw new IllegalArgumentException(
                    "Empty header name at position " + startPos);
            }
            return new PlaceholderToken(PlaceholderType.HEADER, encoding, headerName);
        } else {
            // Simple type with encoding: %{base64:k}, %{hex:v}
            if (encoding == null) {
                throw new IllegalArgumentException(
                    "Only header placeholders can have names. " +
                    "Use %{encoding:type} format for encoded values at position " + startPos);
            }

            PlaceholderType type = switch (remainder) {
                case "k" -> PlaceholderType.KEY;
                case "v" -> PlaceholderType.VALUE;
                case "h" -> PlaceholderType.HEADER;
                default -> throw new IllegalArgumentException(
                    "Unknown placeholder type: " + remainder + " at position " + startPos);
            };

            return new PlaceholderToken(type, encoding, null);
        }
    }
}
