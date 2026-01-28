package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PartitionInfo(
    int id,
    int leader,
    List<Integer> replicas,
    List<Integer> isr,
    List<OffsetInfo> offsets
) {
    @RegisterForReflection
    public static record OffsetInfo(String spec, long value) {
    }

    // Compact constructor with defensive copying
    public PartitionInfo {
        replicas = replicas != null ? List.copyOf(replicas) : List.of();
        isr = isr != null ? List.copyOf(isr) : List.of();
    }
}
