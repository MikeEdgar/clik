package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@RegisterForReflection
public record ConsumedMessage(
    int partition,
    long offset,
    String key,
    String value,
    long timestamp
) {
    /**
     * Create ConsumedMessage from ConsumerRecord
     */
    public static ConsumedMessage from(ConsumerRecord<String, String> rec) {
        return new ConsumedMessage(
                rec.partition(),
                rec.offset(),
                rec.key(),
                rec.value(),
                rec.timestamp()
        );
    }
}
