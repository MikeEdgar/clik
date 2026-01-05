package io.streamshub.clik.support;

/**
 * Represents a literal string in a format string.
 * Literal tokens are matched exactly against the input.
 */
public record LiteralToken(String literal) implements FormatToken {
    public LiteralToken {
        if (literal == null) {
            throw new IllegalArgumentException("Literal cannot be null");
        }
    }
}
