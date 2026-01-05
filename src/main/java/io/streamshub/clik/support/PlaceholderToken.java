package io.streamshub.clik.support;

/**
 * Represents a placeholder in a format string.
 * Placeholders extract values from input lines.
 *
 * Examples:
 * - Simple: %k, %v, %h, %T, %p
 * - Encoded: %{base64:k}, %{hex:v}
 * - Named header: %{h.my-header}, %{base64:h.signature}
 */
public record PlaceholderToken(
    PlaceholderType type,
    String encoding,  // null, "base64", or "hex"
    String name       // for named headers (h.name), null otherwise
) implements FormatToken {
    public PlaceholderToken {
        if (type == null) {
            throw new IllegalArgumentException("Placeholder type cannot be null");
        }
        if (encoding != null && !encoding.equals("base64") && !encoding.equals("hex")) {
            throw new IllegalArgumentException("Invalid encoding: " + encoding);
        }
        if (name != null && type != PlaceholderType.HEADER) {
            throw new IllegalArgumentException("Only header placeholders can have names");
        }
    }

    /**
     * Creates a simple placeholder without encoding or name.
     */
    public static PlaceholderToken simple(PlaceholderType type) {
        return new PlaceholderToken(type, null, null);
    }

    /**
     * Creates an encoded placeholder.
     */
    public static PlaceholderToken encoded(PlaceholderType type, String encoding) {
        return new PlaceholderToken(type, encoding, null);
    }

    /**
     * Creates a named header placeholder.
     */
    public static PlaceholderToken namedHeader(String name) {
        return new PlaceholderToken(PlaceholderType.HEADER, null, name);
    }

    /**
     * Creates a named header placeholder with encoding.
     */
    public static PlaceholderToken namedHeader(String encoding, String name) {
        return new PlaceholderToken(PlaceholderType.HEADER, encoding, name);
    }
}
