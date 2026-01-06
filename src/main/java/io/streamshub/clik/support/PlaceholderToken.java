package io.streamshub.clik.support;

import java.util.Objects;

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
        Objects.requireNonNull(type, "Placeholder type cannot be null");

        if (encoding != null && !encoding.equals("base64") && !encoding.equals("hex")) {
            throw new IllegalArgumentException("Invalid encoding: " + encoding);
        }

        if (name != null && type != PlaceholderType.HEADER) {
            throw new IllegalArgumentException("Only header placeholders can have names");
        }
    }
}
