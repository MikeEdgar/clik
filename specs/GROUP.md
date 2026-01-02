# Kafka Group Management Specification

## Overview

This specification defines group management commands for Clik, enabling users to monitor and manage Kafka consumer groups, share groups, and stream groups. The design follows established patterns from topic and context management while addressing Kafka-specific requirements for group administration.

## Goals

- Provide intuitive CLI commands for common group operations
- Support monitoring of consumer lag and partition assignments
- Enable multiple output formats for machine and human consumption
- Integrate seamlessly with context management for cluster configuration
- Follow kafka-consumer-groups.sh conventions where appropriate for familiarity
- Support all group types in Kafka 4.1+ (consumer, classic, share, streams)

## Non-Goals (Future Enhancements)

- Offset reset operations (v2)
- Consumer group deletion (v2)
- Consumer group rebalancing control (v2)
- Dynamic consumer group quotas (v2)
- Consumer group ACL management (see separate ACL specification)

## User Stories

1. **As a developer**, I want to see which consumer groups exist in a cluster to understand active consumers
2. **As an operator**, I want to view consumer lag to identify processing delays or stuck consumers
3. **As a platform engineer**, I want to see partition assignments to understand consumer distribution
4. **As a DevOps engineer**, I want to monitor group state to detect issues before they impact applications
5. **As a team lead**, I want to identify inactive or empty groups for cleanup

## Command Structure

All group management commands are under the `clik group` subcommand group.

### Command: `clik group list`

List all consumer groups in the cluster.

**Syntax:**
```bash
clik group list [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json, name | table |
| `--type <type>` | Filter by group type: consumer, classic, share, streams | - |

**Examples:**

```bash
# List all groups (table format)
clik group list

# List groups as names only
clik group list -o name

# List only consumer protocol groups
clik group list --type consumer

# List groups as JSON
clik group list -o json
```

**Output (table format):**
```
NAME                   TYPE        STATE      MEMBERS
my-consumer-group      consumer    Stable     3
legacy-group           classic     Stable     2
my-share-group         share       Stable     1
```

**Output (name format):**
```
my-consumer-group
legacy-group
my-share-group
```

**Output (JSON format):**
```json
[
  {
    "groupId": "my-consumer-group",
    "type": "consumer",
    "state": "Stable",
    "members": 3
  },
  {
    "groupId": "legacy-group",
    "type": "classic",
    "state": "Stable",
    "members": 2
  }
]
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. List all groups using `admin.listGroups()`
4. Describe groups to get member count and state
5. Filter by type if `--type` flag is specified
6. Sort groups by name
7. Format output according to `--output` flag

**Group States:**
- `STABLE` - Group is operating normally
- `PREPARING_REBALANCE` - Group is preparing to rebalance
- `COMPLETING_REBALANCE` - Group is completing rebalance
- `DEAD` - Group has no members and metadata has been removed
- `EMPTY` - Group exists but has no members
- `ASSIGNING` - Group is assigning partitions (new consumer protocol)

**Group Types:**
- `consumer` - New KIP-848 consumer protocol group
- `classic` - Traditional consumer group protocol
- `share` - KIP-932 share group (Kafka 4.0+)
- `streams` - Kafka Streams application group

### Command: `clik group describe`

Display detailed information about a consumer group.

**Syntax:**
```bash
clik group describe <groupId> [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json | table |

**Examples:**

```bash
# Describe a group (table format)
clik group describe my-consumer-group

# Output as YAML
clik group describe my-consumer-group -o yaml

# Output as JSON
clik group describe my-consumer-group -o json
```

**Output (table format - Consumer/Classic group):**
```
Group: my-consumer-group
Type: consumer
State: Stable

Members:
MEMBER ID                                      HOST             CLIENT ID           PARTITIONS
consumer-my-consumer-group-1-abc123           /192.168.1.100   consumer-1          my-topic(0,1,2)
consumer-my-consumer-group-2-def456           /192.168.1.101   consumer-2          my-topic(3,4,5)
consumer-my-consumer-group-3-ghi789           /192.168.1.102   consumer-3          my-topic(6,7,8)

Topic Lag:
TOPIC        PARTITION   CURRENT OFFSET   LOG END OFFSET   LAG
my-topic     0           1000             1000             0
my-topic     1           2500             2500             0
my-topic     2           1800             1850             50
my-topic     3           3000             3100             100
my-topic     4           2200             2200             0
my-topic     5           1500             1500             0
my-topic     6           4000             4200             200
my-topic     7           3500             3500             0
my-topic     8           2800             2900             100
```

**Output (table format - Share group):**
```
Group: my-share-group
Type: share
State: Stable

Members:
MEMBER ID                          HOST             CLIENT ID           PARTITIONS
share-consumer-1-abc123           /192.168.1.100   share-client-1      shared-topic(0,1,2,3)
```

**Output (YAML format):**
```yaml
groupId: my-consumer-group
type: consumer
state: Stable
memberCount: 3
coordinator:
  id: 1
  host: broker1.example.com
  port: 9092
members:
  - memberId: consumer-my-consumer-group-1-abc123
    clientId: consumer-1
    host: /192.168.1.100
    assignments:
      - topic: my-topic
        partitions: [0, 1, 2]
  - memberId: consumer-my-consumer-group-2-def456
    clientId: consumer-2
    host: /192.168.1.101
    assignments:
      - topic: my-topic
        partitions: [3, 4, 5]
offsets:
  - topic: my-topic
    partition: 0
    currentOffset: 1000
    logEndOffset: 1000
    lag: 0
  - topic: my-topic
    partition: 1
    currentOffset: 2500
    logEndOffset: 2500
    lag: 0
  - topic: my-topic
    partition: 2
    currentOffset: 1800
    logEndOffset: 1850
    lag: 50
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. Describe the consumer group using `admin.describeConsumerGroups()`
4. Fetch member assignments and partition details
5. For consumer/classic groups: Calculate lag by comparing committed offsets with log end offsets
6. For share/streams groups: Skip lag calculation (not applicable)
7. Format output according to `--output` flag

**Error Conditions:**

- Group does not exist
- No current context set
- Authorization failure

### Command: `clik group delete`

**Status:** ✅ COMPLETED

Delete one or more consumer groups.

**Syntax:**
```bash
clik group delete <groupId> [<groupId>...] [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-y, --yes` | Automatically confirm deletion without prompting | false |

**Examples:**

```bash
# Delete a single group (with confirmation prompt)
clik group delete my-consumer-group

# Delete a single group without confirmation
clik group delete my-consumer-group --yes

# Delete multiple groups
clik group delete group1 group2 group3 --yes
```

**Output:**

```
# Single group
Group "my-consumer-group" deleted.

# Multiple groups
3 groups deleted.
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. If `--yes` is not specified, prompt for confirmation
4. Delete the specified consumer group(s) using `admin.deleteConsumerGroups()`
5. Display success message

**Important Notes:**

- Consumer groups must have no active members to be deleted
- If a group has active consumers, the operation will fail
- Deletion is permanent and cannot be undone
- Internal Kafka groups (prefixed with `__`) cannot be deleted

**Error Conditions:**

- Group does not exist
- Group has active members
- No current context set
- Authorization failure

### Command: `clik group reset-offsets` (Future Enhancement)

**Status:** Not implemented.

Reset consumer group offsets to a specific position. This operation will be added in a future release.

**Planned Options:**
- `--topic <topic>` - Topic to reset
- `--to-earliest` - Reset to earliest offset
- `--to-latest` - Reset to latest offset
- `--to-offset <offset>` - Reset to specific offset
- `--to-datetime <timestamp>` - Reset to timestamp
- `--dry-run` - Show what would be reset without applying changes

**Note:** For now, use kafka-consumer-groups.sh for offset reset operations.

## Integration with Context Management

All group commands support context integration:

**Context Resolution:**

1. Load current context from root config
2. If no current context, return error
3. Merge context config using `common` + `admin` sections
4. Create AdminClient with merged configuration

**Example with contexts:**

```bash
# Set prod context
clik context use prod

# List groups in prod cluster (uses current context)
clik group list

# Describe group in prod
clik group describe my-consumer-group

# Switch to dev and list groups there
clik context use dev
clik group list
```

## Architecture & Implementation

### Package Structure

```
io.streamshub.clik/
├── command/
│   └── group/
│       ├── GroupCommand.java              # Parent @Command
│       ├── ListGroupsCommand.java         # List subcommand
│       └── DescribeGroupCommand.java      # Describe subcommand
├── kafka/
│   ├── KafkaClientFactory.java            # Create AdminClient from context
│   ├── GroupService.java                  # Group operations
│   └── model/
│       ├── GroupInfo.java                 # Group metadata
│       ├── GroupMemberInfo.java           # Member details
│       ├── CoordinatorInfo.java           # Coordinator metadata
│       ├── OffsetLagInfo.java             # Offset and lag data
│       └── GroupType.java                 # Group type enum
└── Clik.java                              # Updated with group subcommand
```

### Core Services

#### GroupService

```java
@ApplicationScoped
public class GroupService {

    /**
     * List all groups, optionally filtered by type
     */
    public Collection<GroupInfo> listGroups(
        Admin admin,
        String typeFilter
    ) throws ExecutionException, InterruptedException;

    /**
     * Describe a specific group with full details
     */
    public GroupInfo describeGroup(
        Admin admin,
        String groupId
    ) throws ExecutionException, InterruptedException;

    /**
     * Get offset and lag information for a consumer group
     */
    private List<OffsetLagInfo> getGroupOffsets(
        Admin admin,
        String groupId
    ) throws ExecutionException, InterruptedException;

    /**
     * Determine group type based on description
     */
    private String determineGroupType(GroupType type);
}
```

**Kafka Admin API Calls:**

- `admin.listGroups()` - List all groups (returns GroupListing with type)
- `admin.describeConsumerGroups(Collection<String> groupIds)` - Get detailed group info
- `admin.listConsumerGroupOffsets(String groupId)` - Get committed offsets
- `admin.listOffsets(Map<TopicPartition, OffsetSpec>)` - Get log end offsets for lag calculation

### Data Models

#### GroupInfo

```java
@RegisterForReflection
public class GroupInfo {
    private String groupId;
    private String type;              // "consumer", "classic", "share", "streams"
    private String state;             // "Stable", "Dead", "Empty", etc.
    private String protocolType;      // For backward compatibility
    private String protocol;          // Partition assignment strategy
    private int memberCount;
    private CoordinatorInfo coordinator;
    private List<GroupMemberInfo> members;      // null for list, populated for describe
    private List<OffsetLagInfo> offsets;        // null for list/non-consumer groups

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        // Builder methods
    }
}
```

#### GroupMemberInfo

```java
@RegisterForReflection
public class GroupMemberInfo {
    private String memberId;
    private String clientId;
    private String host;
    private List<PartitionAssignment> assignments;

    @RegisterForReflection
    public static class PartitionAssignment {
        private String topic;
        private List<Integer> partitions;
    }
}
```

#### CoordinatorInfo

```java
@RegisterForReflection
public class CoordinatorInfo {
    private int id;
    private String host;
    private int port;
}
```

#### OffsetLagInfo

```java
@RegisterForReflection
public class OffsetLagInfo {
    private String topic;
    private int partition;
    private Long currentOffset;   // null if not committed yet
    private Long logEndOffset;
    private Long lag;             // null if currentOffset is null
}
```

#### GroupType

```java
public enum GroupType {
    CONSUMER("consumer"),
    CLASSIC("classic"),
    SHARE("share"),
    STREAMS("streams");

    private final String name;

    public static GroupType fromString(String type);
}
```

## Testing Strategy

### Unit Tests

1. **GroupService Tests (8 tests)**
   - testListGroupsEmpty() - No groups exist
   - testListGroupsConsumer() - List consumer groups
   - testListGroupsFilterByType() - Filter by type
   - testDescribeConsumerGroup() - Describe consumer group
   - testDescribeGroupWithMembers() - Group with active members
   - testDescribeGroupNotFound() - Non-existent group
   - testDescribeGroupOffsets() - Verify lag calculation
   - testDescribeGroupNoOffsets() - Group with no committed offsets

2. **Model Tests**
   - GroupInfo builder pattern
   - GroupType enum parsing
   - Data serialization to JSON/YAML

### Integration Tests

1. **End-to-End Group Flow (13 tests)**
   - List groups (empty, table, name, json, yaml formats)
   - List groups with type filter
   - Describe group (basic, with members, not found)
   - Describe group (json, yaml formats)
   - Describe group with no context
   - Describe group with offsets and lag

2. **Native Integration Tests**
   - GroupCommandIT extends GroupCommandTest
   - Runs all 13 tests against native executable
   - Verifies native image reflection configuration

### Test Infrastructure

- Use Kafka Dev Services (Testcontainers with Strimzi) for integration tests
- Create test consumer groups using KafkaConsumer with specific protocols
- Use longer session timeouts to keep test groups alive during assertions
- Clean up groups after each test using consumer.close()

## Error Messages & User Experience

### Error Message Guidelines

1. **Clear and actionable**
   ```
   Error: Group "my-consumer-group" not found.

   Run 'clik group list' to see available groups.
   ```

2. **Provide context**
   ```
   Error: No current context set.

   Set a context with: clik context use <name>
   ```

3. **Suggest fixes**
   ```
   Error: Failed to describe group: Authorization failed.

   Check that your context has admin permissions for the cluster.
   ```

### Success Messages

```
# List output shown based on format
# Describe output shown based on format
```

## Kafka 4.1 Group Protocol Support

### Consumer Groups (KIP-848)

Kafka 4.1 introduces a new consumer group protocol with:
- Server-side partition assignment
- Incremental cooperative rebalancing by default
- Better scalability for large groups
- `GroupType.CONSUMER` in Admin API

### Classic Groups

Traditional consumer groups still supported:
- Client-side partition assignment
- Stop-the-world rebalancing
- `GroupType.CLASSIC` in Admin API

### Share Groups (KIP-932)

Share groups for competing consumers pattern:
- Multiple consumers can process same partition
- Server-managed offset tracking
- `GroupType.SHARE` in Admin API
- Available in Kafka 4.0+

### Streams Groups

Kafka Streams application groups:
- `GroupType.STREAMS` in Admin API
- Stream thread management
- State store coordination

## Implementation Phases

### Phase 1: Core Group Operations (✅ COMPLETED)
- [x] GroupService implementation with listGroups() and describeGroup()
- [x] GroupInfo, GroupMemberInfo, CoordinatorInfo, OffsetLagInfo models
- [x] GroupType enum for group type handling
- [x] `clik group list` command
- [x] `clik group describe` command
- [x] Unit tests for GroupService (8 tests)
- [x] Integration tests for group commands (13 tests in GroupCommandTest)
- [x] Native integration tests (GroupCommandIT with 13 tests)
- [x] Multiple output formats (table, yaml, json, name)
- [x] Current context integration
- [x] Support for all Kafka 4.1 group types (consumer, classic, share, streams)
- [x] Lag calculation for consumer and classic groups
- [x] GroupIdNotFoundException error handling

### Phase 2: Advanced Group Management (In Progress)
- [x] `clik group delete` command
- [ ] `clik group reset-offsets` command with multiple offset reset strategies
- [ ] Better error messages and validation
- [ ] Performance optimizations for large clusters

### Phase 3: Advanced Features (Future)
- [ ] Consumer group quota management
- [ ] Real-time lag monitoring
- [ ] Group rebalancing controls
- [ ] Consumer group export/import
- [ ] Consumer group metrics/statistics

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka AdminClient API](https://kafka.apache.org/documentation/#adminapi)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [KIP-1043: Administration of groups](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1043%3A+Administration+of+groups)
- [Kafka 4.1 Release Notes](https://downloads.apache.org/kafka/4.1.1/RELEASE_NOTES.html)

## Appendices

### Appendix A: Group State Transitions

Consumer groups can transition through the following states:

```
EMPTY ←→ PREPARING_REBALANCE ←→ COMPLETING_REBALANCE ←→ STABLE
  ↓                                                        ↓
DEAD ←――――――――――――――――――――――――――――――――――――――――――――――――――┘
```

- **EMPTY**: Group has no members
- **PREPARING_REBALANCE**: Members are leaving/joining
- **COMPLETING_REBALANCE**: Partition assignment in progress
- **STABLE**: Normal operation
- **DEAD**: Group is deleted (metadata removed)
- **ASSIGNING**: (New protocol) Server assigning partitions

### Appendix B: Lag Interpretation

**Healthy Consumer:**
- Lag: 0-100 messages
- State: Stable
- All partitions assigned

**Slow Consumer:**
- Lag: Growing over time
- May indicate processing issues
- Check consumer application logs

**Stuck Consumer:**
- Lag: Continuously increasing
- State: May be Dead or Empty
- Consumer likely crashed or disconnected

**No Committed Offsets:**
- currentOffset: null
- New consumer group that hasn't committed yet
- Or auto-commit disabled without manual commits

### Appendix C: Group Type Detection

The group type is determined from the Kafka Admin API:

```java
// Kafka 4.1 Admin API
GroupListing listing = admin.listGroups().all().get();
GroupType type = listing.type().orElse(GroupType.UNKNOWN);

// Maps to our string representation:
// GroupType.CONSUMER   → "consumer"
// GroupType.CLASSIC    → "classic"
// GroupType.SHARE      → "share"
// GroupType.STREAMS    → "streams"
// GroupType.UNKNOWN    → "consumer" (fallback)
```

### Appendix D: Common Use Cases

**Monitor consumer lag:**
```bash
# List all groups
clik group list

# Check lag for specific group
clik group describe my-consumer-group

# Export to JSON for monitoring systems
clik group describe my-consumer-group -o json | jq '.offsets[] | select(.lag > 1000)'
```

**Identify inactive groups:**
```bash
# List all groups with their state
clik group list -o json | jq '.[] | select(.state == "Empty" or .state == "Dead")'
```

**Track group membership:**
```bash
# See which consumers are in a group
clik group describe my-consumer-group | grep -A 100 "Members:"
```
