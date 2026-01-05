package io.streamshub.clik.kafka.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record KafkaRecord(
    Integer partition,
    Long offset,
    ByteArray key,
    ByteArray value,
    Long timestamp,
    List<Header> headers
) {
    @RegisterForReflection
    public static record Header(
        String key,
        ByteArray value
    ) {
        Header(String key, byte[] value) {
            this(key, new ByteArray(value));
        }

        public String valueString(String defaultValue) {
            return value.stringValue(defaultValue);
        }

        public byte[] valueBytes() {
            return value.bytesValue();
        }
    }

    private static record ByteArray(byte[] value) {
        ByteArray {
            value = value != null ? value.clone() : null;
        }

        @Override
        public final boolean equals(Object other) {
            return other instanceof ByteArray(byte[] otherValue) && Arrays.equals(otherValue, value);
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public final String toString() {
            return "ByteArray[value=" + Arrays.toString(value) + "]";
        }

        String stringValue(String defaultValue) {
            return value != null ? new String(value, StandardCharsets.UTF_8) : defaultValue;
        }

        byte[] bytesValue() {
            return value != null ? value.clone() : null;
        }
    }

    private KafkaRecord(Integer partition, Long offset, byte[] key, byte[] value, Long timestamp,
            List<Header> headers) {
        this(partition, offset, new ByteArray(key), new ByteArray(value), timestamp, headers);
    }

    public String keyString(String defaultValue) {
        return key.stringValue(defaultValue);
    }

    public byte[] keyBytes() {
        return key.bytesValue();
    }

    public String valueString(String defaultValue) {
        return value.stringValue(defaultValue);
    }

    public byte[] valueBytes() {
        return value.bytesValue();
    }

    private Stream<Header> matchingHeaders(String key) {
        return headers.stream().filter(h -> h.key.equals(key));
    }

    public List<Header> headers(String key) {
        return matchingHeaders(key).toList();
    }

    public Header firstHeader(String key) {
        return matchingHeaders(key).findFirst().orElse(null);
    }

    /**
     * Create ConsumedMessage from ConsumerRecord
     */
    public static KafkaRecord from(ConsumerRecord<byte[], byte[]> rec) {
        List<Header> headers = new ArrayList<>();
        for (var header : rec.headers()) {
            headers.add(new Header(header.key(), header.value()));
        }

        return new KafkaRecord(
                rec.partition(),
                rec.offset(),
                rec.key(),
                rec.value(),
                rec.timestamp(),
                headers
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        Integer partition;
        Long offset;
        byte[] key;
        byte[] value;
        Long timestamp;
        List<Header> headers = new ArrayList<>();

        private Builder() {
            // use KafkaRecord.builder()
        }

        public void setKey(byte[] key) {
            this.key = key;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public void addHeader(String name, byte[] value) {
            headers.add(new Header(name, value));
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public KafkaRecord build() {
            return new KafkaRecord(partition, offset, key, value, timestamp, headers);
        }
    }
}
