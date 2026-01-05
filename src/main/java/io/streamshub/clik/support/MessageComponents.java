package io.streamshub.clik.support;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents the components of a Kafka message parsed from an input line.
 * This class holds the key, value, headers, timestamp, and partition
 * extracted from a single line of input using a format string.
 */
public class MessageComponents {
    private byte[] key;
    private byte[] value;
    private final Map<String, byte[]> headers = new LinkedHashMap<>();
    private Long timestamp;
    private Integer partition;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Map<String, byte[]> getHeaders() {
        return headers;
    }

    public void addHeader(String name, byte[] value) {
        headers.put(name, value);
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }
}
