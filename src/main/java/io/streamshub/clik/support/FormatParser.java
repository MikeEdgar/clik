package io.streamshub.clik.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parser for format strings used in the --input option.
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
public class FormatParser {

    private FormatParser() {
        // Static utility class
    }

    /**
     * Parse a format string into a ParsedFormat ready to match input lines.
     *
     * @param format The format string
     * @return Parsed format
     * @throws IllegalArgumentException if the format string is invalid
     */
    public static ParsedFormat parse(String format) {
        if (format == null || format.isEmpty()) {
            throw new IllegalArgumentException("Format string cannot be null or empty");
        }

        // First, process unicode escapes
        format = processUnicodeEscapes(format);

        List<FormatToken> tokens = new ArrayList<>();
        Set<String> namedHeaders = new HashSet<>();
        boolean hasGenericHeader = false;
        boolean hasPlaceholder = false;

        int i = 0;
        StringBuilder literal = new StringBuilder();

        while (i < format.length()) {
            if (format.charAt(i) == '%') {
                // Save preceding literal
                if (literal.length() > 0) {
                    tokens.add(new LiteralToken(literal.toString()));
                    literal.setLength(0);
                }

                i++; // skip '%'

                if (i >= format.length()) {
                    throw new IllegalArgumentException("Format string ends with incomplete placeholder");
                }

                if (format.charAt(i) == '%') {
                    // Escaped percent
                    literal.append('%');
                    i++;
                } else if (format.charAt(i) == '{') {
                    // Parameterized placeholder: %{encoding:type.name}
                    i++; // skip '{'
                    int closeBrace = format.indexOf('}', i);
                    if (closeBrace == -1) {
                        throw new IllegalArgumentException("Unclosed placeholder: " + format.substring(i - 2));
                    }

                    String spec = format.substring(i, closeBrace);
                    PlaceholderToken token = parseParameterizedPlaceholder(spec);
                    tokens.add(token);
                    hasPlaceholder = true;

                    // Validate header rules
                    if (token.type() == PlaceholderType.HEADER) {
                        if (token.name() != null) {
                            if (namedHeaders.contains(token.name())) {
                                throw new IllegalArgumentException("Duplicate header: " + token.name());
                            }
                            namedHeaders.add(token.name());
                        } else {
                            if (hasGenericHeader) {
                                throw new IllegalArgumentException("Generic header placeholder can only appear once");
                            }
                            hasGenericHeader = true;
                        }
                    }

                    i = closeBrace + 1;
                } else {
                    // Simple placeholder: %k, %v, %h, %T, %p
                    PlaceholderToken token = parseSimplePlaceholder(format.charAt(i));
                    tokens.add(token);
                    hasPlaceholder = true;

                    if (token.type() == PlaceholderType.HEADER) {
                        if (hasGenericHeader) {
                            throw new IllegalArgumentException("Generic header placeholder can only appear once");
                        }
                        hasGenericHeader = true;
                    }

                    i++;
                }
            } else {
                literal.append(format.charAt(i));
                i++;
            }
        }

        // Save trailing literal
        if (literal.length() > 0) {
            tokens.add(new LiteralToken(literal.toString()));
        }

        if (!hasPlaceholder) {
            throw new IllegalArgumentException("Format string must contain at least one placeholder");
        }

        return new ParsedFormat(tokens, namedHeaders, hasGenericHeader);
    }

    /**
     * Process unicode escape sequences ({@literal \}uXXXX) in the format string.
     */
    private static String processUnicodeEscapes(String format) {
        StringBuilder result = new StringBuilder();
        int i = 0;

        while (i < format.length()) {
            if (i + 5 < format.length() &&
                format.charAt(i) == '\\' &&
                format.charAt(i + 1) == 'u') {
                // Unicode escape
                try {
                    String hex = format.substring(i + 2, i + 6);
                    int codePoint = Integer.parseInt(hex, 16);
                    result.append((char) codePoint);
                    i += 6;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "Invalid unicode escape: " + format.substring(i, Math.min(i + 6, format.length())));
                }
            } else {
                result.append(format.charAt(i));
                i++;
            }
        }

        return result.toString();
    }

    /**
     * Parse a simple placeholder character into a PlaceholderToken.
     */
    private static PlaceholderToken parseSimplePlaceholder(char c) {
        PlaceholderType type = switch (c) {
            case 'k' -> PlaceholderType.KEY;
            case 'v' -> PlaceholderType.VALUE;
            case 'h' -> PlaceholderType.HEADER;
            case 'T' -> PlaceholderType.TIMESTAMP;  // uppercase T
            case 'p' -> PlaceholderType.PARTITION;
            default -> throw new IllegalArgumentException("Unknown placeholder: %" + c);
        };
        return PlaceholderToken.simple(type);
    }

    /**
     * Parse a parameterized placeholder specification into a PlaceholderToken.
     *
     * Patterns:
     * - "base64:k" -> encoding=base64, type=KEY, name=null
     * - "h.my-header" -> encoding=null, type=HEADER, name=my-header
     * - "base64:h.my-header" -> encoding=base64, type=HEADER, name=my-header
     */
    private static PlaceholderToken parseParameterizedPlaceholder(String spec) {
        if (spec.isEmpty()) {
            throw new IllegalArgumentException("Empty placeholder specification");
        }

        String encoding = null;
        String rest = spec;

        // Check for encoding prefix
        if (spec.contains(":")) {
            String[] parts = spec.split(":", 2);
            encoding = parts[0];
            if (!encoding.equals("base64") && !encoding.equals("hex")) {
                throw new IllegalArgumentException("Unknown encoding: " + encoding);
            }
            rest = parts[1];
        }

        // Parse type and name
        String typeStr;
        String name = null;

        if (rest.contains(".")) {
            String[] parts = rest.split("\\.", 2);
            typeStr = parts[0];
            name = parts[1];

            if (name.isEmpty()) {
                throw new IllegalArgumentException("Empty header name");
            }
        } else {
            typeStr = rest;
        }

        PlaceholderType type = switch (typeStr) {
            case "k" -> PlaceholderType.KEY;
            case "v" -> PlaceholderType.VALUE;
            case "h" -> PlaceholderType.HEADER;
            case "T" -> PlaceholderType.TIMESTAMP;  // uppercase T
            case "p" -> PlaceholderType.PARTITION;
            default -> throw new IllegalArgumentException("Unknown placeholder type: " + typeStr);
        };

        // Only headers can have names
        if (name != null && type != PlaceholderType.HEADER) {
            throw new IllegalArgumentException("Only header placeholders can have names");
        }

        // Create appropriate token
        if (type == PlaceholderType.HEADER && name != null) {
            return PlaceholderToken.namedHeader(encoding, name);
        } else if (encoding != null) {
            return PlaceholderToken.encoded(type, encoding);
        } else {
            return PlaceholderToken.simple(type);
        }
    }
}
