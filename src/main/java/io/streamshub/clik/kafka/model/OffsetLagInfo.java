package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record OffsetLagInfo(
    String topic,
    int partition,
    Long currentOffset,   // null if not committed yet
    Long logEndOffset,
    Long lag             // null if currentOffset is null
) {}
