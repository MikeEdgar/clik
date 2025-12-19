package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;
import java.util.Map;

@RegisterForReflection
public class TopicInfo {
    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean internal;
    private Map<String, String> config;
    private List<PartitionInfo> partitionDetails;

    public TopicInfo() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public List<PartitionInfo> getPartitionDetails() {
        return partitionDetails;
    }

    public void setPartitionDetails(List<PartitionInfo> partitionDetails) {
        this.partitionDetails = partitionDetails;
    }

    public static class Builder {
        private final TopicInfo topicInfo = new TopicInfo();

        public Builder name(String name) {
            topicInfo.name = name;
            return this;
        }

        public Builder partitions(int partitions) {
            topicInfo.partitions = partitions;
            return this;
        }

        public Builder replicationFactor(int replicationFactor) {
            topicInfo.replicationFactor = replicationFactor;
            return this;
        }

        public Builder internal(boolean internal) {
            topicInfo.internal = internal;
            return this;
        }

        public Builder config(Map<String, String> config) {
            topicInfo.config = config;
            return this;
        }

        public Builder partitionDetails(List<PartitionInfo> partitionDetails) {
            topicInfo.partitionDetails = partitionDetails;
            return this;
        }

        public TopicInfo build() {
            return topicInfo;
        }
    }
}
