package io.streamshub.clik.kafka.model;

import java.util.List;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Represents Kafka cluster information including nodes and optional quorum details
 */
@RegisterForReflection
public record ClusterInfo(
    String clusterId,
    String featureLevel,
    int controllerId,
    List<NodeInfo> nodes,
    QuorumInfo quorumInfo  // null in ZooKeeper mode
) {

    // Compact constructor with defensive copying
    public ClusterInfo {
        nodes = nodes != null ? List.copyOf(nodes) : List.of();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String clusterId;
        private String featureLevel;
        private int controllerId;
        private List<NodeInfo> nodes;
        private QuorumInfo quorumInfo;

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder featureLevel(String featureLevel) {
            this.featureLevel = featureLevel;
            return this;
        }

        public Builder controllerId(int controllerId) {
            this.controllerId = controllerId;
            return this;
        }

        public Builder nodes(List<NodeInfo> nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder quorumInfo(QuorumInfo quorumInfo) {
            this.quorumInfo = quorumInfo;
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(clusterId, featureLevel, controllerId, nodes, quorumInfo);
        }
    }

    /**
     * Metadata quorum information (KRaft only)
     */
    @RegisterForReflection
    public record QuorumInfo(
        int leaderId,
        long leaderEpoch,
        long highWatermark,
        List<ObserverState> observers
    ) {
        // Compact constructor with defensive copying
        public QuorumInfo {
            observers = observers != null ? List.copyOf(observers) : List.of();
        }
    }

    /**
     * Observer node state in the metadata quorum
     */
    @RegisterForReflection
    public record ObserverState(
        int nodeId,
        long lastFetchTimestamp,
        long lastCaughtUpTimestamp
    ) {}
}
