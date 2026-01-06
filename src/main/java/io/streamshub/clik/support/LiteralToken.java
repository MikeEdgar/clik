package io.streamshub.clik.support;

import java.util.Objects;

/**
 * Represents a literal string in a format string.
 * Literal tokens are matched exactly against the input.
 */
public record LiteralToken(String literal) implements FormatToken {
    public LiteralToken {
        Objects.requireNonNull(literal, "Literal cannot be null");
    }
}
