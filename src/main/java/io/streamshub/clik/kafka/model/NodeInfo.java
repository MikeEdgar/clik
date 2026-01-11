package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Represents a Kafka cluster node (broker/controller)
 */
@RegisterForReflection
public record NodeInfo(
    int id,
    String host,
    int port,
    String rack,
    NodeRole role,
    QuorumRole quorumRole,
    boolean isLeader
) {

    /**
     * Basic node role from describeCluster()
     */
    @RegisterForReflection
    public enum NodeRole {
        BROKER,           // Broker-only node
        CONTROLLER,       // Controller-only node (rare)
        COMBINED;         // Combined broker+controller node (KRaft)

        @Override
        public String toString() {
            return switch (this) {
                case BROKER -> "Broker";
                case CONTROLLER -> "Controller";
                case COMBINED -> "Broker+Controller";
            };
        }
    }

    /**
     * Quorum participation role from describeMetadataQuorum() (KRaft only)
     */
    @RegisterForReflection
    public enum QuorumRole {
        VOTER,      // Voter in metadata quorum
        OBSERVER,   // Observer in metadata quorum
        NONE;       // Not in quorum (broker-only in KRaft, or ZooKeeper mode)

        @Override
        public String toString() {
            return switch (this) {
                case VOTER -> "Voter";
                case OBSERVER -> "Observer";
                case NONE -> "-";
            };
        }

        /**
         * Get display string for quorum role with optional leader annotation
         */
        public String toDisplayString(boolean isLeader) {
            if (this == VOTER && isLeader) {
                return "Voter (Leader)";
            }
            return toString();
        }
    }

    /**
     * Get display string for quorum role (for table output)
     */
    public String quorumRoleDisplay() {
        return quorumRole != null ? quorumRole.toDisplayString(isLeader) : "-";
    }
}
