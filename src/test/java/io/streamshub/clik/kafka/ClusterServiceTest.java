package io.streamshub.clik.kafka;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.kafka.model.ClusterInfo;
import io.streamshub.clik.kafka.model.NodeInfo;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class ClusterServiceTest extends ClikTestBase {

    @Inject
    ClusterService clusterService;

    @Test
    void testDescribeClusterBasicInfo() throws Exception {
        ClusterInfo cluster = clusterService.describeCluster(admin());

        assertNotNull(cluster, "Cluster info should not be null");
        assertNotNull(cluster.clusterId(), "Cluster ID should not be null");
        assertFalse(cluster.clusterId().isEmpty(), "Cluster ID should not be empty");
        assertTrue(cluster.controllerId() >= 0, "Controller ID should be non-negative");
        assertNotNull(cluster.nodes(), "Nodes list should not be null");
        assertFalse(cluster.nodes().isEmpty(), "Nodes list should not be empty");
    }

    @Test
    void testNodeInfoValidation() throws Exception {
        ClusterInfo cluster = clusterService.describeCluster(admin());

        for (NodeInfo node : cluster.nodes()) {
            assertTrue(node.id() >= 0, "Node ID should be non-negative");
            assertNotNull(node.host(), "Node host should not be null");
            assertFalse(node.host().isEmpty(), "Node host should not be empty");
            assertTrue(node.port() > 0, "Node port should be positive");
            assertNotNull(node.role(), "Node role should not be null");
        }
    }

    @Test
    void testControllerPresence() throws Exception {
        ClusterInfo cluster = clusterService.describeCluster(admin());

        int controllerId = cluster.controllerId();
        boolean controllerFound = cluster.nodes().stream()
                .anyMatch(node -> node.id() == controllerId);

        assertTrue(controllerFound, "Controller ID should be found in nodes list");
    }

    @Test
    void testNodesSortedById() throws Exception {
        ClusterInfo cluster = clusterService.describeCluster(admin());

        var nodes = cluster.nodes();
        for (int i = 1; i < nodes.size(); i++) {
            assertTrue(nodes.get(i - 1).id() < nodes.get(i).id(),
                    "Nodes should be sorted by ID in ascending order");
        }
    }

    @Test
    void testQuorumInfoInKRaftMode() throws Exception {
        ClusterInfo cluster = clusterService.describeCluster(admin());

        // This test handles both ZooKeeper and KRaft modes
        if (cluster.quorumInfo() != null) {
            // KRaft mode - verify quorum information is present
            assertNotNull(cluster.quorumInfo(), "Quorum info should not be null in KRaft mode");
            assertTrue(cluster.quorumInfo().leaderId() >= 0, "Leader ID should be non-negative");
            assertTrue(cluster.quorumInfo().leaderEpoch() >= 0, "Leader epoch should be non-negative");
            assertTrue(cluster.quorumInfo().highWatermark() >= 0, "High watermark should be non-negative");
            assertNotNull(cluster.quorumInfo().observers(), "Observers list should not be null");
        }
        // In ZooKeeper mode, quorumInfo will be null, which is expected
    }

    @Test
    void testQuorumRolesInKRaftMode() throws Exception {
        ClusterInfo cluster = clusterService.describeCluster(admin());

        // This test handles both ZooKeeper and KRaft modes
        if (cluster.quorumInfo() != null) {
            // KRaft mode - verify nodes have quorum roles
            boolean hasVoterOrObserver = cluster.nodes().stream()
                    .anyMatch(node -> node.quorumRole() == NodeInfo.QuorumRole.VOTER
                            || node.quorumRole() == NodeInfo.QuorumRole.OBSERVER);

            assertTrue(hasVoterOrObserver, "At least one node should be a VOTER or OBSERVER in KRaft mode");

            // Verify leader flag is set for the leader node
            int leaderId = cluster.quorumInfo().leaderId();
            var leaderNode = cluster.nodes().stream()
                    .filter(node -> node.id() == leaderId)
                    .findFirst();

            if (leaderNode.isPresent()) {
                assertTrue(leaderNode.get().isLeader(), "Leader node should have isLeader flag set");
                assertEquals(NodeInfo.QuorumRole.VOTER, leaderNode.get().quorumRole(),
                        "Leader should be a VOTER");
            }
        } else {
            // ZooKeeper mode - all nodes should have null quorum role
            assertTrue(cluster.nodes().stream().allMatch(node -> node.quorumRole() == null),
                    "All nodes should have null quorum role in ZooKeeper mode");
        }
    }
}
