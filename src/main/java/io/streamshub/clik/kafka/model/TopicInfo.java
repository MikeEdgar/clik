package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;
import java.util.Map;

@RegisterForReflection
public record TopicInfo(
    String name,
    int partitions,
    int replicationFactor,
    boolean internal,
    Map<String, String> config,
    List<PartitionInfo> partitionDetails
) {
    // Compact constructor with defensive copying
    public TopicInfo {
        config = config != null ? Map.copyOf(config) : Map.of();
        partitionDetails = partitionDetails != null ? List.copyOf(partitionDetails) : List.of();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private int partitions;
        private int replicationFactor;
        private boolean internal;
        private Map<String, String> config;
        private List<PartitionInfo> partitionDetails;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder partitions(int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder replicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public Builder internal(boolean internal) {
            this.internal = internal;
            return this;
        }

        public Builder config(Map<String, String> config) {
            this.config = config;
            return this;
        }

        public Builder partitionDetails(List<PartitionInfo> partitionDetails) {
            this.partitionDetails = partitionDetails;
            return this;
        }

        public TopicInfo build() {
            return new TopicInfo(name, partitions, replicationFactor,
                               internal, config, partitionDetails);
        }
    }
}
