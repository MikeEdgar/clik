package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public record PartitionInfo(
    int id,
    int leader,
    List<Integer> replicas,
    List<Integer> isr
) {
    // Compact constructor with defensive copying
    public PartitionInfo {
        replicas = replicas != null ? List.copyOf(replicas) : List.of();
        isr = isr != null ? List.copyOf(isr) : List.of();
    }
}
