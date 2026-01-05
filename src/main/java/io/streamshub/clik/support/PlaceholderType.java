package io.streamshub.clik.support;

/**
 * Types of placeholders supported in format strings.
 */
public enum PlaceholderType {
    /**
     * Message key (%k)
     */
    KEY,

    /**
     * Message value (%v)
     */
    VALUE,

    /**
     * Message header (%h or %{h.name})
     */
    HEADER,

    /**
     * Message timestamp (%T)
     */
    TIMESTAMP,

    /**
     * Target partition (%p)
     */
    PARTITION
}
