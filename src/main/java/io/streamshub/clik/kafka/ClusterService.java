package io.streamshub.clik.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumResult;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.QuorumInfo.ReplicaState;
import org.apache.kafka.common.Node;
import org.apache.kafka.server.common.MetadataVersion;
import org.jboss.logging.Logger;

import io.streamshub.clik.config.ConfigurationLoader;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.model.ClusterInfo;
import io.streamshub.clik.kafka.model.NodeInfo;
import io.streamshub.clik.kafka.model.NodeInfo.NodeRole;
import io.streamshub.clik.kafka.model.NodeInfo.QuorumRole;

@ApplicationScoped
public class ClusterService {

    @Inject
    Logger logger;

    @Inject
    ContextService contextService;

    @Inject
    ConfigurationLoader configurationLoader;

    /**
     * Describe the Kafka cluster including nodes and optional quorum information.
     * Derive the Kafka version (feature level) from the metadata version feature.
     *
     * @param admin Admin client
     * @return ClusterInfo containing cluster details
     * @throws ExecutionException if the operation fails
     * @throws InterruptedException if the operation is interrupted
     */
    public ClusterInfo describeCluster(Admin admin)
            throws ExecutionException, InterruptedException {

        // Step 1: Get basic cluster information
        DescribeClusterResult clusterResult = admin.describeCluster();

        String clusterId = clusterResult.clusterId().get();
        Node controller = clusterResult.controller().get();
        Set<Node> nodes = new HashSet<>(clusterResult.nodes().get());

        int controllerId = controller != null ? controller.id() : -1;

        // Step 2: Try to get quorum information (KRaft only)
        ClusterInfo.QuorumInfo quorumInfo = null;
        Set<Integer> brokerIds = nodes.stream().map(Node::id).collect(Collectors.toSet());
        Set<Integer> voterIds = new HashSet<>();
        Set<Integer> observerIds = new HashSet<>();
        int quorumLeaderId = -1;

        try {
            DescribeMetadataQuorumResult quorumResult = admin.describeMetadataQuorum();
            QuorumInfo kafkaQuorumInfo = quorumResult.quorumInfo().toCompletionStage().toCompletableFuture().join();

            // Extract voter and observer IDs
            for (ReplicaState voter : kafkaQuorumInfo.voters()) {
                voterIds.add(voter.replicaId());
            }

            for (ReplicaState observer : kafkaQuorumInfo.observers()) {
                observerIds.add(observer.replicaId());
            }

            kafkaQuorumInfo.nodes().values().stream()
                .map(n -> {
                    if (nodes.stream().noneMatch(node -> node.id() == n.nodeId())) {
                        return new Node(n.nodeId(), null, -1);
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(nodes::add);

            quorumLeaderId = kafkaQuorumInfo.leaderId();

            // Build QuorumInfo with observer states
            List<ClusterInfo.ObserverState> observerStates = new ArrayList<>();
            for (ReplicaState observer : kafkaQuorumInfo.observers()) {
                observerStates.add(new ClusterInfo.ObserverState(
                        observer.replicaId(),
                        observer.lastFetchTimestamp().orElse(-1L),
                        observer.lastCaughtUpTimestamp().orElse(-1L)
                ));
            }

            quorumInfo = new ClusterInfo.QuorumInfo(
                    kafkaQuorumInfo.leaderId(),
                    kafkaQuorumInfo.leaderEpoch(),
                    kafkaQuorumInfo.highWatermark(),
                    observerStates
            );
        } catch (Exception e) {
            // ZooKeeper mode (quorum information not available or other errors (e.g., authorization).
            // Log and continue without quorum info
            logger.infof("Exception retrieving Metadata Quorum: %s", e.toString());
        }

        // Step 3: Convert nodes to NodeInfo with role detection
        List<NodeInfo> nodeInfos = new ArrayList<>();
        for (Node node : nodes) {
            NodeRole basicRole = determineBasicRole(node.id(), voterIds, observerIds, brokerIds, controllerId);
            QuorumRole quorumRole = determineQuorumRole(node.id(), voterIds, observerIds, quorumInfo != null);
            boolean isLeader = (node.id() == quorumLeaderId);

            nodeInfos.add(new NodeInfo(
                    node.id(),
                    node.host(),
                    node.port(),
                    node.rack(),
                    basicRole,
                    quorumRole,
                    isLeader
            ));
        }

        // Sort nodes by ID
        nodeInfos.sort(Comparator.comparingInt(NodeInfo::id));

        return ClusterInfo.builder()
                .clusterId(clusterId)
                .featureLevel(determineFeatureLevel(admin.describeFeatures()))
                .controllerId(controllerId)
                .nodes(nodeInfos)
                .quorumInfo(quorumInfo)
                .build();
    }

    /* testing */ String determineFeatureLevel(DescribeFeaturesResult features) {
        var metadataVersionMax = Optional.ofNullable(features
                .featureMetadata()
                .toCompletionStage()
                .toCompletableFuture()
                .join()
                .finalizedFeatures()
                .get(MetadataVersion.FEATURE_NAME))
                .map(FinalizedVersionRange::maxVersionLevel);

        return metadataVersionMax
                .map(level -> {
                    try {
                        return MetadataVersion.fromFeatureLevel(level).shortVersion();
                    } catch (IllegalArgumentException _) {
                        if (level < MetadataVersion.MINIMUM_VERSION.featureLevel()) {
                            return "Unknown (<%s)".formatted(MetadataVersion.MINIMUM_VERSION.shortVersion());
                        }

                        return "Unknown (>%s)".formatted(MetadataVersion.latestTesting().shortVersion());
                    }
                })
                .orElse("Unknown");
    }

    /**
     * Determine basic node role from cluster information
     *
     * @param nodeId Node ID
     * @param controllerId Current controller ID
     * @return Basic role (BROKER, CONTROLLER, or COMBINED)
     */
    private NodeRole determineBasicRole(int nodeId,
            Set<Integer> voterIds,
            Set<Integer> observerIds,
            Set<Integer> brokerIds,
            int controllerId) {

        if (voterIds.contains(nodeId)) {
            return brokerIds.contains(nodeId) ? NodeRole.COMBINED : NodeRole.CONTROLLER;
        }

        if (observerIds.contains(nodeId) || brokerIds.contains(nodeId)) {
            return NodeRole.BROKER;
        }

        if (nodeId == controllerId) {
            // Node is the current controller
            // In KRaft, controllers may also be brokers (COMBINED)
            // describeCluster().nodes() returns broker nodes, so if it's in
            // the list and is controller, it's COMBINED
            return NodeRole.COMBINED;
        }
        // All other nodes in the nodes() list are brokers
        return NodeRole.BROKER;
    }

    /**
     * Determine quorum participation role from quorum information
     *
     * @param nodeId Node ID
     * @param voterIds Set of voter IDs
     * @param observerIds Set of observer IDs
     * @param hasQuorumInfo Whether quorum information is available
     * @return Quorum role (VOTER, OBSERVER, or NONE)
     */
    private QuorumRole determineQuorumRole(int nodeId, Set<Integer> voterIds,
                                           Set<Integer> observerIds, boolean hasQuorumInfo) {
        if (!hasQuorumInfo) {
            // ZooKeeper mode - no quorum information
            return null;
        }

        if (voterIds.contains(nodeId)) {
            return QuorumRole.VOTER;
        } else if (observerIds.contains(nodeId)) {
            return QuorumRole.OBSERVER;
        } else {
            // Not in quorum - broker-only node in KRaft
            return QuorumRole.NONE;
        }
    }
}
