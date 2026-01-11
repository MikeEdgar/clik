# Kafka Cluster Management Specification

## Overview

This specification defines cluster management commands for Clik, enabling users to view detailed information about Kafka clusters. The design provides comprehensive cluster visibility including node details, controller information, and KRaft quorum metadata.

## Goals

- Provide detailed cluster information for operators and developers
- Support both KRaft and ZooKeeper deployment modes
- Display node roles with distinction between brokers, controllers, and combined nodes
- Show quorum metadata for KRaft clusters (voters, observers, leader)
- Enable multiple output formats for machine and human consumption
- Integrate seamlessly with context management for cluster configuration

## Non-Goals (Future Enhancements)

- Cluster health metrics and monitoring (v2)
- Broker configuration management (v2)
- Cluster topology visualization (v2)
- Partition rebalancing controls (v2)
- Cluster-wide metrics and performance data (v2)

## User Stories

1. **As an operator**, I want to view basic cluster information to verify connectivity and cluster identity
2. **As a platform engineer**, I want to see node details to understand cluster topology
3. **As a DevOps engineer**, I want to identify controller nodes in KRaft mode for troubleshooting
4. **As a site reliability engineer**, I want to view quorum metadata to monitor cluster metadata health
5. **As an administrator**, I want to export cluster information as JSON/YAML for documentation

## Command Structure

All cluster management commands are under the `clik cluster` subcommand group.

### Command: `clik cluster describe`

Display detailed information about the Kafka cluster including nodes and quorum metadata.

**Syntax:**
```bash
clik cluster describe [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json | table |

**Examples:**

```bash
# Describe cluster (table format)
clik cluster describe

# Describe cluster as JSON
clik cluster describe -o json

# Describe cluster as YAML
clik cluster describe -o yaml
```

**Output (table format - KRaft mode):**
```
Cluster ID: xyzABC123
Feature Level: 4.1
Controller ID: 1
Nodes: 3

Metadata Quorum:
  Leader ID: 1
  Controller Epoch: 45
  High Watermark: 12345

Cluster Nodes:
ID  HOST              PORT   RACK     ROLE               QUORUM ROLE
 0  broker-0.kafka    9092   rack-1   Broker             Voter
 1  broker-1.kafka    9092   rack-2   Broker+Controller  Voter (Leader)
 2  broker-2.kafka    9092   rack-3   Broker             Observer
```

**Output (table format - ZooKeeper mode):**
```
Cluster ID: abcXYZ789
Feature Level: 3.9
Controller ID: 0
Nodes: 3

Cluster Nodes:
ID  HOST              PORT   RACK     ROLE
 0  broker-0.kafka    9092   rack-1   Broker+Controller
 1  broker-1.kafka    9092   rack-2   Broker
 2  broker-2.kafka    9092   rack-3   Broker
```

**Output (JSON format - KRaft mode):**
```json
{
  "clusterId": "xyzABC123",
  "featureLevel": "4.1",
  "controllerId": 1,
  "nodes": [
    {
      "id": 0,
      "host": "broker-0.kafka",
      "port": 9092,
      "rack": "rack-1",
      "role": "BROKER",
      "quorumRole": "VOTER",
      "isLeader": false
    },
    {
      "id": 1,
      "host": "broker-1.kafka",
      "port": 9092,
      "rack": "rack-2",
      "role": "COMBINED",
      "quorumRole": "VOTER",
      "isLeader": true
    },
    {
      "id": 2,
      "host": "broker-2.kafka",
      "port": 9092,
      "rack": "rack-3",
      "role": "BROKER",
      "quorumRole": "OBSERVER",
      "isLeader": false
    }
  ],
  "quorumInfo": {
    "leaderId": 1,
    "leaderEpoch": 45,
    "highWatermark": 12345,
    "observers": [
      {
        "nodeId": 2,
        "lastFetchTimestamp": 1704729600000,
        "lastCaughtUpTimestamp": 1704729550000
      }
    ]
  }
}
```

**Output (YAML format - KRaft mode):**
```yaml
clusterId: "xyzABC123"
featureLevel: "4.1"
controllerId: 1
nodes:
  - id: 0
    host: "broker-0.kafka"
    port: 9092
    rack: "rack-1"
    role: "BROKER"
    quorumRole: "VOTER"
    isLeader: false
  - id: 1
    host: "broker-1.kafka"
    port: 9092
    rack: "rack-2"
    role: "COMBINED"
    quorumRole: "VOTER"
    isLeader: true
  - id: 2
    host: "broker-2.kafka"
    port: 9092
    rack: "rack-3"
    role: "BROKER"
    quorumRole: "OBSERVER"
    isLeader: false
quorumInfo:
  leaderId: 1
  leaderEpoch: 45
  highWatermark: 12345
  observers:
    - nodeId: 2
      lastFetchTimestamp: 1704729600000
      lastCaughtUpTimestamp: 1704729550000
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. Call `describeCluster()` to get basic cluster information (cluster ID, controller, nodes)
4. Call `describeFeatures()` to get Kafka feature level version
5. Try to call `describeMetadataQuorum()` for KRaft-specific metadata
   - If successful (KRaft mode): Extract voter/observer IDs, leader, epoch, high watermark
   - If `UnsupportedOperationException` (ZooKeeper mode): Continue without quorum metadata
6. Merge node information with role detection:
   - **Basic Role** (from describeCluster):
     - `BROKER` - Broker-only node
     - `CONTROLLER` - Controller-only node (rare in KRaft)
     - `COMBINED` - Combined broker+controller node (common in KRaft)
   - **Quorum Role** (from describeMetadataQuorum, KRaft only):
     - `VOTER` - Voting member of metadata quorum
     - `OBSERVER` - Observer member of metadata quorum
     - `NONE` - Not in quorum (broker-only node)
     - `null` - ZooKeeper mode (quorum info not available)
7. Sort nodes by ID for consistent output
8. Format and display based on output format option

**Error Conditions:**

- No current context set
- Connection failure to Kafka cluster
- Authorization failure (insufficient permissions)
- Invalid output format specified

## Implementation Details

### Data Models

**ClusterInfo** (record with builder):
- `String clusterId` - Unique cluster identifier
- `String featureLevel` - Kafka feature level version (e.g., "4.1", "3.9")
- `int controllerId` - Current controller node ID
- `List<NodeInfo> nodes` - List of cluster nodes
- `QuorumInfo quorumInfo` - Quorum metadata (null in ZooKeeper mode)

**NodeInfo** (record):
- `int id` - Node ID
- `String host` - Node hostname
- `int port` - Node port
- `String rack` - Rack ID (null if not configured)
- `NodeRole role` - Basic node role (BROKER, CONTROLLER, COMBINED)
- `QuorumRole quorumRole` - Quorum participation role (VOTER, OBSERVER, NONE, or null)
- `boolean isLeader` - True if this node is the current quorum leader

**QuorumInfo** (nested record in ClusterInfo):
- `int leaderId` - Current quorum leader node ID
- `long leaderEpoch` - Current leader epoch
- `long highWatermark` - High watermark offset
- `List<ObserverState> observers` - Observer node states

**ObserverState** (nested record in ClusterInfo):
- `int nodeId` - Observer node ID
- `long lastFetchTimestamp` - Last fetch timestamp (-1 if not available)
- `long lastCaughtUpTimestamp` - Last caught up timestamp (-1 if not available)

### Service Layer

**ClusterService** (`@ApplicationScoped`):
- `ClusterInfo describeCluster(Admin admin)` - Main describe operation
  - Performs dual API call strategy
  - Merges cluster and quorum information
  - Handles ZooKeeper mode gracefully

### Role Detection Logic

**Two-tier role detection strategy:**

1. **Basic Role Detection** (from describeCluster):
   - If node is in voters list: `COMBINED` (if also in brokers) or `CONTROLLER` (if not)
   - If node is in observers list or brokers list: `BROKER`
   - If node ID matches controller ID: `COMBINED`
   - Default: `BROKER`

2. **Quorum Role Detection** (from describeMetadataQuorum, KRaft only):
   - If in voters list: `VOTER`
   - If in observers list: `OBSERVER`
   - If not in quorum: `NONE`
   - If ZooKeeper mode: `null`

3. **Leader Annotation**:
   - Set `isLeader = true` for node matching `quorumInfo.leaderId`
   - Display as "Voter (Leader)" in table output

### KRaft vs ZooKeeper Mode

**KRaft Mode** (Kafka 4.0+):
- `describeMetadataQuorum()` succeeds
- Quorum metadata section displayed
- Two-column node table (ROLE + QUORUM ROLE)
- Voters, observers, and leader clearly identified

**ZooKeeper Mode** (Kafka 3.x and earlier):
- `describeMetadataQuorum()` throws `UnsupportedOperationException`
- No quorum metadata section
- Single-column node table (ROLE only)
- Basic broker/controller distinction

### Output Formatting

**Table Format:**
- Cluster summary section (ID, feature level, controller, node count)
- Quorum metadata section (KRaft only)
- Node table with columns:
  - KRaft: ID, HOST, PORT, RACK, ROLE, QUORUM ROLE
  - ZooKeeper: ID, HOST, PORT, RACK, ROLE
- Uses ascii-table library for formatting

**JSON Format:**
- Pretty-printed JSON with all fields
- Includes quorumInfo when available
- Suitable for automation and scripting

**YAML Format:**
- YAML format with all fields
- Omits document start marker for cleaner output
- Suitable for configuration management

## Testing Strategy

### Unit Tests (ClusterServiceTest)

1. **testDescribeClusterBasicInfo** - Verify cluster ID, controller ID, nodes not empty
2. **testNodeInfoValidation** - Verify all nodes have valid ID, host, port, role
3. **testControllerPresence** - Verify controller ID found in nodes list
4. **testNodesSortedById** - Verify nodes sorted in ascending ID order
5. **testQuorumInfoInKRaftMode** - Verify quorum info present in KRaft mode (or null in ZooKeeper)
6. **testQuorumRolesInKRaftMode** - Verify nodes have quorum roles in KRaft mode (or null in ZooKeeper)

### Integration Tests (ClusterCommandTest)

1. **testDescribeClusterTable** - Verify table output contains expected headers and data
2. **testDescribeClusterJson** - Verify JSON output contains all required fields
3. **testDescribeClusterYaml** - Verify YAML output contains all required fields
4. **testDescribeClusterInvalidFormat** - Verify error handling for invalid format
5. **testDescribeClusterMissingContext** - Verify error when no context set
6. **testClusterCommandHasDescribeSubcommand** - Verify describe subcommand registered
7. **testClusterCommandHasHelpSubcommand** - Verify help subcommand registered

### Integration Tests (ClusterCommandIT)

- Inherits all tests from ClusterCommandTest
- Runs against packaged JAR or native executable

## Future Enhancements

### Cluster Health Command (v2)

Dedicated `clik cluster health` command providing:
- Partition leadership distribution
- Under-replicated partitions count
- Offline partitions count
- ISR (In-Sync Replica) health metrics
- Broker availability status
- Metadata lag metrics (KRaft)

**Example:**
```bash
clik cluster health
clik cluster health -o json
```

### Cluster Configuration (v2)

View and manage cluster-wide configurations:
- `clik cluster config list` - List all cluster configs
- `clik cluster config get <key>` - Get specific config value
- `clik cluster config set <key=value>` - Update cluster config (dangerous)

### Broker Management (v2)

Individual broker information and operations:
- `clik broker list` - List all brokers with details
- `clik broker describe <id>` - Detailed broker information
- `clik broker config <id>` - Broker-specific configuration

### Cluster Metrics (v2)

Real-time cluster metrics:
- Message throughput (messages/sec, bytes/sec)
- Request latency (produce, fetch, metadata)
- Partition count and distribution
- Topic count and size

### Topology Visualization (v2)

Visual representation of cluster topology:
- Rack awareness visualization
- Partition placement diagrams
- Replication factor heatmaps
- Leadership distribution charts

## Common Use Cases

### Verify Cluster Connectivity

```bash
# Quick cluster info check
clik cluster describe

# Verify controller is healthy
clik cluster describe | grep "Controller ID"
```

### Troubleshoot KRaft Cluster

```bash
# Check quorum leader and voters
clik cluster describe

# Export for analysis
clik cluster describe -o json > cluster-state.json

# Verify all voters are present
clik cluster describe -o yaml | grep -A 10 "quorumInfo"
```

### Document Cluster Topology

```bash
# Generate cluster documentation
clik cluster describe -o yaml > docs/cluster-topology.yaml

# Compare topologies across environments
diff <(clik cluster describe -o json --context dev) \
     <(clik cluster describe -o json --context prod)
```

### Audit Node Configuration

```bash
# List all nodes with roles
clik cluster describe -o json | jq '.nodes[] | {id, host, role, quorumRole}'

# Find all controller nodes
clik cluster describe -o json | jq '.nodes[] | select(.role == "COMBINED" or .role == "CONTROLLER")'

# Identify quorum leader
clik cluster describe -o json | jq '.nodes[] | select(.isLeader == true)'
```

### Monitor Cluster Changes

```bash
# Watch for controller changes
watch -n 5 "clik cluster describe | grep 'Controller ID'"

# Track quorum leadership changes
while true; do
  clik cluster describe -o json | jq '.quorumInfo.leaderId'
  sleep 10
done
```

## References

- Kafka Admin API: `describeCluster()`, `describeMetadataQuorum()`, `describeFeatures()`
- KRaft documentation: [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
- Kafka 4.1 release notes
