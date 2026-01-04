package io.streamshub.clik.kafka.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record KafkaRecord(
    int partition,
    long offset,
    String key,
    String value,
    long timestamp,
    List<Header> headers
) {
    @RegisterForReflection
    public record Header(
        String key,
        String value
    ) {
    }

    /**
     * Create ConsumedMessage from ConsumerRecord
     */
    public static KafkaRecord from(ConsumerRecord<String, String> rec) {
        List<Header> headers = new ArrayList<>();
        for (var header : rec.headers()) {
            String headerKey = header.key();
            String headerValue = header.value() != null
                    ? new String(header.value(), StandardCharsets.UTF_8)
                    : null;
            headers.add(new Header(headerKey, headerValue));
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
}
