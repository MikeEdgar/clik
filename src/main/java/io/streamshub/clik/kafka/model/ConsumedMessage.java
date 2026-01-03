package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@RegisterForReflection
public class ConsumedMessage {
    private int partition;
    private long offset;
    private String key;
    private String value;
    private long timestamp;

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create ConsumedMessage from ConsumerRecord
     */
    public static ConsumedMessage from(ConsumerRecord<String, String> rec) {
        return builder()
                .partition(rec.partition())
                .offset(rec.offset())
                .key(rec.key())
                .value(rec.value())
                .timestamp(rec.timestamp())
                .build();
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public static class Builder {
        private final ConsumedMessage message = new ConsumedMessage();

        public Builder partition(int partition) {
            message.partition = partition;
            return this;
        }

        public Builder offset(long offset) {
            message.offset = offset;
            return this;
        }

        public Builder key(String key) {
            message.key = key;
            return this;
        }

        public Builder value(String value) {
            message.value = value;
            return this;
        }

        public Builder timestamp(long timestamp) {
            message.timestamp = timestamp;
            return this;
        }

        public ConsumedMessage build() {
            return message;
        }
    }
}
