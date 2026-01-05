package io.streamshub.clik.support;

/**
 * Marker interface for tokens in a parsed format string.
 * A format string consists of literal text and placeholders.
 */
public sealed interface FormatToken permits LiteralToken, PlaceholderToken {
}
