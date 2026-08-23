package io.streamshub.clik.kafka.model;

import java.util.Optional;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record OffsetLagInfo(
    String topic,
    int partition,
    Long currentOffset,   // null if not committed yet
    Long logEndOffset,
    Long lag             // null if currentOffset is null
) {

    public Optional<Long> optionalCurrentOffset() {
        return Optional.ofNullable(currentOffset);
    }

    public Optional<Long> optionalLogEndOffset() {
        return Optional.ofNullable(logEndOffset);
    }

    public Optional<Long> optionalLag() {
        return Optional.ofNullable(lag);
    }

}
