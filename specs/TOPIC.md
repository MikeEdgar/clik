# Kafka Topic Management Specification

## Overview

This specification defines topic management commands for Clik, enabling users to perform CRUD operations on Kafka topics. The design follows the established patterns from context management while addressing Kafka-specific requirements for topic administration.

## Goals

- Provide intuitive CLI commands for common topic operations
- Support both simple and advanced topic configurations
- Enable multiple output formats for machine and human consumption
- Integrate seamlessly with context management for cluster configuration
- Follow kafka-topics.sh conventions where appropriate for familiarity

## Non-Goals (Future Enhancements)

- Topic ACL management (see separate ACL specification)
- Quota management (see separate quota specification)
- Mirror/replication management (v2)
- Topic templates or presets (v2)
- Bulk topic operations from file (v2)

## User Stories

1. **As a developer**, I want to create topics for my application without remembering complex partition/replication settings
2. **As an operator**, I want to list all topics in a cluster to audit what exists
3. **As a platform engineer**, I want to view topic configurations to troubleshoot issues
4. **As a DevOps engineer**, I want to update topic configurations for performance tuning
5. **As a team lead**, I want to delete old/unused topics to clean up the cluster

## Command Structure

All topic management commands are under the `clik topic` subcommand group.

### Command: `clik topic create`

Create a new Kafka topic.

**Syntax:**
```bash
clik topic create <name> [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-p, --partitions <count>` | Number of partitions | 1 |
| `-r, --replication-factor <count>` | Replication factor | 1 |
| `-c, --config <key=value>` | Topic configuration (repeatable) | - |

**Examples:**

```bash
# Create simple topic with defaults
clik topic create my-topic

# Create topic with partitions and replication
clik topic create events --partitions 6 --replication-factor 3

# Create topic with custom configuration
clik topic create logs \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config compression.type=zstd
```

**Behavior:**

1. Load configuration from current context
2. Validate topic name (Kafka naming rules)
3. Check if topic already exists (error if exists)
4. Create AdminClient with context configuration
5. Create topic with specified partitions, replication factor, and configs
6. Print success message

**Output:**
```
Topic "test-topic" created.
```

**Error Conditions:**

- Topic name is invalid (contains invalid characters)
- Topic already exists
- Insufficient brokers for replication factor
- Invalid configuration keys or values
- No current context set
- Authorization failure

### Command: `clik topic list`

List all topics in the cluster.

**Syntax:**
```bash
clik topic list [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json, name | table |
| `--internal` | Include internal topics (__consumer_offsets, etc.) | false |

**Examples:**

```bash
# List all topics (table format)
clik topic list

# List topics as names only
clik topic list -o name

# List all topics including internal
clik topic list --internal

# List topics as JSON
clik topic list -o json
```

**Output (table format):**
```
NAME              PARTITIONS   REPLICATION FACTOR
events            6            3
logs              12           3
user-activity     3            2
```

**Output (name format):**
```
events
logs
user-activity
```

**Output (JSON format):**
```json
[
  {
    "name": "events",
    "partitions": 6,
    "replicationFactor": 3,
    "internal": false
  },
  {
    "name": "logs",
    "partitions": 12,
    "replicationFactor": 3,
    "internal": false
  }
]
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. List topics (optionally including internal topics)
4. Fetch partition and replication metadata for all topics
5. Sort topics by name
6. Format output according to `--output` flag

### Command: `clik topic describe`

Display detailed information about a topic, including partition details.

**Syntax:**
```bash
clik topic describe <name> [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json | table |

**Examples:**

```bash
# Describe a topic (table format)
clik topic describe events

# Output as YAML
clik topic describe events -o yaml

# Output as JSON
clik topic describe events -o json
```

**Output (table format):**
```
Topic: events
Partitions: 6
Replication Factor: 3
Internal: no

Configuration:
  retention.ms = 604800000
  compression.type = zstd

Partition Details:
PARTITION   LEADER   REPLICAS      ISR
0           1        [1, 2, 3]     [1, 2, 3]
1           2        [2, 3, 1]     [2, 3, 1]
2           3        [3, 1, 2]     [3, 1, 2]
3           1        [1, 2, 3]     [1, 2, 3]
4           2        [2, 3, 1]     [2, 3, 1]
5           3        [3, 1, 2]     [3, 1, 2]
```

**Output (YAML format):**
```yaml
name: events
partitions: 6
replicationFactor: 3
internal: false
config:
  retention.ms: "604800000"
  compression.type: "zstd"
partitionDetails:
  - id: 0
    leader: 1
    replicas: [1, 2, 3]
    isr: [1, 2, 3]
  - id: 1
    leader: 2
    replicas: [2, 3, 1]
    isr: [2, 3, 1]
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. Fetch topic metadata (partitions, replicas)
4. Fetch topic configuration (non-default values only)
5. Fetch detailed partition information (partition ID, leader, replicas, ISR)
6. Format output according to `--output` flag

### Command: `clik topic update`

Update topic configuration.

**Syntax:**
```bash
clik topic update <name> [OPTIONS]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--config <key=value>` | Set configuration (repeatable) |
| `--delete-config <key>` | Delete configuration (repeatable) |
| `--context <name>` | Use specific context |

**Examples:**

```bash
# Update topic retention
clik topic update events --config retention.ms=172800000

# Update multiple configs
clik topic update events \
  --config retention.ms=172800000 \
  --config compression.type=lz4

# Delete a configuration (revert to default)
clik topic update events --delete-config compression.type

# Update and delete in one operation
clik topic update events \
  --config min.insync.replicas=2 \
  --delete-config max.message.bytes
```

**Output:**
```
Topic "events" configuration updated.
```

**Behavior:**

1. Load configuration from current or specified context
2. Validate topic exists
3. Create AdminClient with context configuration
4. Apply configuration changes (incremental alter configs)
5. Print success message

**Note:** Partition count and replication factor cannot be changed via update. Use separate partition/replication commands for those operations.

**Error Conditions:**

- Topic does not exist
- Invalid configuration keys or values
- Authorization failure

### Command: `clik topic delete`

Delete one or more topics.

**Syntax:**
```bash
clik topic delete <name> [<name>...] [OPTIONS]
```

**Options:**

| Flag | Description |
|------|-------------|
| `-f, --force` | Skip confirmation prompt |

**Examples:**

```bash
# Delete a topic (with confirmation)
clik topic delete old-topic

# Delete multiple topics
clik topic delete topic1 topic2 topic3

# Delete without confirmation
clik topic delete old-topic --force
```

**Output (with confirmation):**
```
Delete topic "old-topic"? This cannot be undone. [y/N]: y
Topic "old-topic" deleted.
```

**Output (without confirmation):**
```
Topic "old-topic" deleted.
```

**Behavior:**

1. Load configuration from current or specified context
2. Validate topics exist
3. Prompt for confirmation (unless `--force`)
4. Create AdminClient with context configuration
5. Delete topics
6. Print success message for each topic

**Error Conditions:**

- Topic does not exist
- Authorization failure
- Topic deletion disabled (delete.topic.enable=false)
- User declines confirmation

### Command: `clik topic partitions`

Manage topic partitions.

**Syntax:**
```bash
# Add partitions to a topic
clik topic partitions <name> --add <count> [OPTIONS]

# Show current partition count
clik topic partitions <name>
```

**Options:**

| Flag | Description |
|------|-------------|
| `--add <count>` | Add partitions (new total count) |
| `--context <name>` | Use specific context |

**Examples:**

```bash
# Show current partition count
clik topic partitions events

# Increase partitions to 12 (from current count)
clik topic partitions events --add 12
```

**Output (show):**
```
Topic "events" has 6 partitions.
```

**Output (add):**
```
Topic "events" partitions increased from 6 to 12.
```

**Behavior:**

1. Load configuration from current or specified context
2. Create AdminClient with context configuration
3. Fetch current partition count
4. If `--add` specified:
   - Validate new count is greater than current
   - Create new partitions
   - Print success message
5. Otherwise, print current count

**Note:** Partitions can only be added, never removed. Kafka does not support reducing partition count.

**Error Conditions:**

- Topic does not exist
- New partition count is less than or equal to current count
- Authorization failure

## Integration with Context Management

All topic commands support the `--context` flag to use a specific context, or will use the current context if not specified.

**Context Resolution:**

1. If `--context <name>` provided, load that context configuration
2. Otherwise, load current context from root config
3. If no current context, require explicit `--bootstrap-servers` flag (future enhancement)
4. Merge context config using `common` + `admin` sections

**Example with contexts:**

```bash
# Set prod context
clik context use prod

# Create topic in prod cluster (uses current context)
clik topic create important-events --partitions 12 --replication-factor 3

# List topics in dev cluster (override context)
clik topic list --context dev

# Describe topic in staging
clik topic describe events --context staging
```

## Architecture & Implementation

### Package Structure

```
io.streamshub.clik/
├── command/
│   ├── topic/
│   │   ├── TopicCommand.java              # Parent @Command
│   │   ├── CreateTopicCommand.java
│   │   ├── ListTopicsCommand.java
│   │   ├── DescribeTopicCommand.java
│   │   ├── UpdateTopicCommand.java
│   │   ├── DeleteTopicCommand.java
│   │   └── PartitionsCommand.java
├── kafka/
│   ├── KafkaClientFactory.java            # Create AdminClient from context
│   ├── TopicService.java                  # Topic CRUD operations
│   └── model/
│       ├── TopicInfo.java                 # Topic metadata
│       └── PartitionInfo.java             # Partition metadata
└── Clik.java                              # Updated with topic subcommand
```

### Core Services

#### KafkaClientFactory

```java
@ApplicationScoped
public class KafkaClientFactory {

    @Inject
    ContextService contextService;

    @Inject
    ConfigurationLoader configurationLoader;

    /**
     * Create AdminClient from current context
     */
    public AdminClient createAdminClient() {
        Optional<String> currentContext = contextService.getCurrentContext();
        if (!currentContext.isPresent()) {
            throw new IllegalStateException("No current context set");
        }
        return createAdminClient(currentContext.get());
    }

    /**
     * Create AdminClient from specific context
     */
    public AdminClient createAdminClient(String contextName) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, KafkaClientType.ADMIN);
        return AdminClient.create(props);
    }
}
```

#### TopicService

```java
@ApplicationScoped
public class TopicService {

    /**
     * Create a new topic
     */
    public void createTopic(
        AdminClient admin,
        String name,
        int partitions,
        int replicationFactor,
        Map<String, String> configs
    );

    /**
     * List all topics
     */
    public List<TopicInfo> listTopics(AdminClient admin, boolean includeInternal);

    /**
     * Describe topic(s)
     */
    public List<TopicInfo> describeTopics(AdminClient admin, List<String> names);

    /**
     * Update topic configuration
     */
    public void updateTopicConfig(
        AdminClient admin,
        String name,
        Map<String, String> configs,
        List<String> deleteConfigs
    );

    /**
     * Delete topic(s)
     */
    public void deleteTopics(AdminClient admin, List<String> names);

    /**
     * Add partitions to topic
     */
    public void addPartitions(AdminClient admin, String name, int newTotal);
}
```

### Data Models

#### TopicInfo

```java
public class TopicInfo {
    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean internal;
    private Map<String, String> config;
    private List<PartitionInfo> partitionDetails; // Optional, for --partitions flag

    // Getters, setters, builder
}
```

#### PartitionInfo

```java
public class PartitionInfo {
    private int id;
    private int leader;
    private List<Integer> replicas;
    private List<Integer> isr;

    // Getters, setters
}
```

## Testing Strategy

### Unit Tests

1. **TopicService Tests**
   - Create topic with valid config
   - Create topic that already exists
   - List topics (empty, single, multiple)
   - List topics with internal topics
   - Describe topic
   - Update topic config
   - Delete topic
   - Add partitions

2. **KafkaClientFactory Tests**
   - Create admin client from current context
   - Create admin client from specific context
   - Error when no current context

### Integration Tests

1. **End-to-End Topic Flow**
   - Create context → create topic → list → describe → update → delete
   - Create multiple topics and list
   - Describe multiple topics
   - Add partitions to topic

2. **Error Handling**
   - Create topic with invalid name
   - Create duplicate topic
   - Delete non-existent topic
   - Update non-existent topic

3. **Output Formats**
   - List topics in all formats (table, yaml, json, name)
   - Describe topics in all formats
   - Verify JSON/YAML structure matches specification

### Test Infrastructure

- Use embedded Kafka (kafka-junit or testcontainers) for integration tests
- Create test contexts with test cluster configuration
- Clean up topics after each test

## Error Messages & User Experience

### Error Message Guidelines

1. **Clear and actionable**
   ```
   Error: Topic "events" already exists.

   View topic details with: clik topic describe events
   Or delete the existing topic with: clik topic delete events
   ```

2. **Provide context**
   ```
   Error: Replication factor 5 is greater than available brokers (3).

   Reduce --replication-factor to 3 or fewer.
   ```

3. **Suggest fixes**
   ```
   Error: No current context set.

   Set a context with: clik context use <name>
   Or specify bootstrap servers: --bootstrap-servers localhost:9092
   ```

### Success Messages

```
Topic "events" created with 6 partitions and replication factor 3.
Topic "events" deleted.
Topic "events" configuration updated.
Topic "events" partitions increased from 6 to 12.
```

### Interactive Prompts

```bash
$ clik topic delete events
Delete topic "events"? This cannot be undone. [y/N]:
```

## Configuration Examples

### Example 1: Simple Development Topic

```bash
clik topic create dev-events --partitions 3
```

### Example 2: Production Topic with Tuning

```bash
clik topic create prod-events \
  --partitions 24 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=zstd \
  --config min.insync.replicas=2 \
  --config max.message.bytes=1048576
```

### Example 3: Compacted Topic

```bash
clik topic create user-state \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.5 \
  --config segment.ms=86400000
```

## Implementation Phases

### Phase 1: Core Topic Operations (Completed)
- [x] KafkaClientFactory implementation
- [x] TopicService implementation with CRUD operations
- [x] TopicInfo and PartitionInfo model classes (with @RegisterForReflection)
- [x] `clik topic create` command
- [x] `clik topic list` command
- [x] `clik topic describe` command
- [x] `clik topic delete` command
- [x] Unit tests for KafkaClientFactory (4 tests)
- [x] Unit tests for TopicService (10 tests)
- [x] Integration tests for topic commands (15 tests in TopicCommandTest)
- [x] Integration test suite (TopicCommandIT with 15 tests)
- [x] Multiple output formats (table, yaml, json, name)
- [x] Current context integration

### Phase 2: Advanced Topic Management (Future)
- [ ] `clik topic update` command for configuration changes
- [ ] `clik topic partitions` command for adding partitions
- [ ] `--context` flag to override current context
- [ ] Better error messages and validation
- [ ] Performance optimizations for large clusters

### Phase 3: Advanced Features (Future)
- [ ] Topic templates/presets
- [ ] Bulk operations from file
- [ ] Topic dry-run mode
- [ ] Topic cloning/copying
- [ ] Topic metrics/statistics

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka AdminClient API](https://kafka.apache.org/documentation/#adminapi)
- [Kafka Topic Configuration](https://kafka.apache.org/documentation/#topicconfigs)
- [Picocli User Manual](https://picocli.info/)

## Appendices

### Appendix A: Topic Name Validation Rules

Kafka topic names must:
- Contain only: letters (a-z, A-Z), numbers (0-9), periods (.), underscores (_), hyphens (-)
- Not consist only of periods
- Not be empty
- Be at most 249 characters long

**Validation Pattern:**
```java
private static final Pattern TOPIC_NAME_PATTERN =
    Pattern.compile("^[a-zA-Z0-9._-]+$");

private static boolean isValidTopicName(String name) {
    if (name == null || name.isEmpty() || name.length() > 249) {
        return false;
    }
    if (name.equals(".") || name.equals("..")) {
        return false;
    }
    return TOPIC_NAME_PATTERN.matcher(name).matches();
}
```

### Appendix B: Common Topic Configurations

**Retention Settings:**
- `retention.ms` - Time to retain messages (default: 7 days)
- `retention.bytes` - Maximum total size of log before old segments are deleted

**Performance Settings:**
- `compression.type` - Compression codec (none, gzip, snappy, lz4, zstd)
- `max.message.bytes` - Maximum size of a message batch
- `segment.ms` - Time before a new log segment is rolled
- `segment.bytes` - Size of a single log segment

**Replication Settings:**
- `min.insync.replicas` - Minimum replicas that must acknowledge a write
- `unclean.leader.election.enable` - Allow out-of-sync replicas to become leader

**Compaction Settings:**
- `cleanup.policy` - delete or compact
- `min.cleanable.dirty.ratio` - Minimum ratio of dirty data for compaction
- `delete.retention.ms` - How long to retain delete tombstone markers

### Appendix C: Default Topic Configurations

```yaml
# Development defaults
partitions: 3
replication-factor: 1
config:
  retention.ms: 604800000  # 7 days

# Production defaults
partitions: 6
replication-factor: 3
config:
  retention.ms: 604800000  # 7 days
  min.insync.replicas: 2
  compression.type: zstd
```
