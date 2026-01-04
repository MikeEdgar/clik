package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public record GroupMemberInfo(
    String memberId,
    String clientId,
    String host,
    List<PartitionAssignment> assignments
) {
    // Compact constructor with defensive copying
    public GroupMemberInfo {
        assignments = assignments != null ? List.copyOf(assignments) : List.of();
    }

    @RegisterForReflection
    public record PartitionAssignment(
        String topic,
        List<Integer> partitions
    ) {
        // Compact constructor with defensive copying
        public PartitionAssignment {
            partitions = partitions != null ? List.copyOf(partitions) : List.of();
        }
    }
}
