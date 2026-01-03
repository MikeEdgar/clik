# Kafka Producer and Consumer Commands Specification

## Overview

This specification defines producer and consumer commands for Clik, enabling users to produce messages to Kafka topics and consume messages from topics. These commands complete the "data flow" story for the MVP release, allowing users to interact with message data directly through the CLI.

## Goals

- Provide simple, intuitive commands for producing and consuming messages
- Support common use cases for message inspection and testing
- Enable multiple input sources for producing (file, stdin, interactive)
- Support flexible consumption modes (one-time, continuous, offset control)
- Integrate seamlessly with context management for cluster configuration
- Use String serialization for simplicity (MVP scope)

## Non-Goals (Future Enhancements)

- Custom serializers/deserializers (Avro, Protobuf, JSON Schema)
- Transactional producer support
- Headers manipulation
- Compression configuration
- Advanced consumer features (manual partition assignment with rebalancing)
- Message filtering or transformation
- Batch consumption with grouping

## User Stories

### Producer Stories

1. **As a developer**, I want to produce test messages to a topic from a file for integration testing
2. **As an operator**, I want to quickly send a message to a topic for debugging
3. **As a tester**, I want to produce messages with specific keys to test partitioning logic
4. **As a developer**, I want to produce messages to a specific partition for testing edge cases

### Consumer Stories

1. **As a developer**, I want to consume messages from a topic to verify production
2. **As an operator**, I want to view the latest messages in a topic for monitoring
3. **As a debugger**, I want to consume from a specific offset to reproduce an issue
4. **As a tester**, I want to consume messages in different formats (JSON, YAML) for automation
5. **As an operator**, I want to continuously monitor a topic for new messages

## Command Structure

### Command: `clik produce`

Produce messages to a Kafka topic.

**Syntax:**
```bash
clik produce <topic> [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-f, --file <path>` | Read messages from file (one message per line) | - |
| `-i, --interactive` | Interactive mode (prompt for messages) | - |
| `-k, --key <key>` | Message key (applied to all messages) | null |
| `-p, --partition <num>` | Target partition number | - |

**Examples:**

```bash
# Produce messages from a file
clik produce my-topic --file messages.txt

# Produce messages from stdin (via pipe)
echo -e "msg1\nmsg2\nmsg3" | clik produce my-topic

# Produce with a specific key
clik produce my-topic --file messages.txt --key user123

# Produce to a specific partition
clik produce my-topic --file messages.txt --partition 2

# Interactive mode
clik produce my-topic --interactive
```

**Output:**
```
3 messages sent successfully
```

**Behavior:**

1. Load configuration from current context
2. Create KafkaProducer with String serializer
3. Read messages from input source (file, stdin, or interactive)
4. Send each message as a ProducerRecord
5. Flush producer to ensure all messages are sent
6. Report success/failure counts

**Input Sources Priority:**
1. If `--file` specified: read from file
2. Else if `--interactive` specified: read from System.in with prompt
3. Else: read from stdin (piped input)

**Important Notes:**
- Each line in the input is treated as a separate message
- Empty lines produce empty messages
- File and interactive modes are mutually exclusive
- Messages are sent synchronously for reliability
- All messages use the same key if `--key` is specified

**Error Conditions:**
- File not found
- No current context set
- File and interactive both specified
- Topic does not exist (auto-creation disabled)
- Producer configuration errors
- Network/broker errors

### Command: `clik consume`

Consume messages from a Kafka topic.

**Syntax:**
```bash
clik consume <topic> [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-g, --group <id>` | Consumer group ID | generated unique ID |
| `-f, --follow` | Continuous mode (consume until interrupted) | false |
| `--from-beginning` | Start from earliest offset | - |
| `--from-end` | Start from latest offset | - |
| `--from-offset <offset>` | Start from specific offset (requires --partition) | - |
| `-p, --partition <num>` | Consume from specific partition only | all partitions |
| `-o, --output <format>` | Output format: table, json, yaml, value | table |
| `--max-messages <num>` | Maximum messages to consume | unlimited |
| `--timeout <ms>` | Timeout in milliseconds for one-time consumption | 5000 |

**Examples:**

```bash
# Consume from beginning (one-time read)
clik consume my-topic --from-beginning

# Consume latest messages
clik consume my-topic --from-end --timeout 10000

# Continuous consumption (Ctrl+C to stop)
clik consume my-topic --from-beginning --follow

# Consume with consumer group
clik consume my-topic --group my-app --from-beginning

# Consume from specific offset and partition
clik consume my-topic --partition 0 --from-offset 1000

# Consume specific partition only
clik consume my-topic --partition 2 --from-beginning

# Limit number of messages
clik consume my-topic --from-beginning --max-messages 10

# Output as JSON
clik consume my-topic --from-beginning -o json

# Output values only (no metadata)
clik consume my-topic --from-beginning -o value
```

**Output (table format):**
```
 PARTITION  OFFSET  KEY       VALUE
         0      42  user123   Hello World
         0      43            Test message
         1      28  user456   Another message
```

**Output (JSON format):**
```json
[
  {
    "partition": 0,
    "offset": 42,
    "key": "user123",
    "value": "Hello World",
    "timestamp": 1704326400000
  },
  {
    "partition": 0,
    "offset": 43,
    "key": null,
    "value": "Test message",
    "timestamp": 1704326401000
  }
]
```

**Output (YAML format):**
```yaml
- partition: 0
  offset: 42
  key: "user123"
  value: "Hello World"
  timestamp: 1704326400000
- partition: 0
  offset: 43
  key: null
  value: "Test message"
  timestamp: 1704326401000
```

**Output (value format):**
```
Hello World
Test message
Another message
```

**Behavior:**

1. Load configuration from current context
2. Generate or use provided consumer group ID
3. Create KafkaConsumer with String deserializer
4. Assign partitions and seek to appropriate offset
5. Poll for messages until timeout or max messages reached
6. Format and display messages according to output format
7. Close consumer cleanly

**Offset Behavior:**
- **Standalone mode (no --group)**: Defaults to `--from-end` to avoid consuming all historical messages
- **Consumer group mode (with --group)**: Defaults to Kafka-managed offsets (resumes from last committed)
- **Explicit offset options**: Override defaults

**Continuous Mode (--follow):**
- Polls continuously until interrupted (Ctrl+C)
- Prints messages as they arrive
- Registers shutdown hook for clean exit
- Respects `--max-messages` limit if specified

**Important Notes:**
- Only one offset option can be specified (--from-beginning, --from-end, --from-offset)
- `--from-offset` requires `--partition` to be specified
- In continuous mode, timeout is ignored
- Standalone consumers (no group ID) don't commit offsets
- Consumer group consumers track offsets but this command doesn't commit them

**Error Conditions:**
- Topic does not exist
- No current context set
- Multiple offset options specified
- `--from-offset` without `--partition`
- Invalid partition number
- Invalid output format
- Consumer configuration errors
- Network/broker errors

## Architecture & Implementation

### Package Structure

```
io.streamshub.clik/
├── command/
│   ├── produce/
│   │   └── ProduceCommand.java           # Produce command implementation
│   └── consume/
│       └── ConsumeCommand.java           # Consume command implementation
├── kafka/
│   ├── KafkaClientFactory.java           # Modified: added createProducer() and createConsumer()
│   └── model/
│       └── ConsumedMessage.java          # Model for consumed messages
└── Clik.java                             # Updated with produce/consume subcommands
```

### Core Services

#### KafkaClientFactory Extensions

```java
@ApplicationScoped
public class KafkaClientFactory {

    /**
     * Create KafkaProducer from current context
     */
    public Producer<String, String> createProducer() {
        Optional<String> currentContext = contextService.getCurrentContext();
        if (!currentContext.isPresent()) {
            throw new IllegalStateException("No current context set...");
        }
        return createProducer(currentContext.get());
    }

    public Producer<String, String> createProducer(String contextName) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, KafkaClientType.PRODUCER);

        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    /**
     * Create KafkaConsumer from current context
     */
    public Consumer<String, String> createConsumer(String groupId) {
        Optional<String> currentContext = contextService.getCurrentContext();
        if (!currentContext.isPresent()) {
            throw new IllegalStateException("No current context set...");
        }
        return createConsumer(currentContext.get(), groupId);
    }

    public Consumer<String, String> createConsumer(String contextName, String groupId) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, KafkaClientType.CONSUMER);

        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                          StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                          StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(props);
    }
}
```

### Data Models

#### ConsumedMessage

```java
@RegisterForReflection
public class ConsumedMessage {
    private int partition;
    private long offset;
    private String key;
    private String value;
    private long timestamp;

    public static ConsumedMessage from(ConsumerRecord<String, String> record) {
        return builder()
            .partition(record.partition())
            .offset(record.offset())
            .key(record.key())
            .value(record.value())
            .timestamp(record.timestamp())
            .build();
    }

    // Builder pattern, getters, setters
}
```

## Testing Strategy

### ProduceCommand Tests

**Test Coverage (8 tests):**

1. `testProduceFromStdin()` - Produce messages from stdin (disabled for QuarkusMainTest)
2. `testProduceFromFile()` - Produce messages from file
3. `testProduceWithKey()` - Verify key assignment
4. `testProduceWithPartition()` - Verify partition targeting
5. `testProduceNoContext()` - Error when no context
6. `testProduceFileNotFound()` - Error handling
7. `testProduceMultipleMessages()` - Batch sending (10 messages)
8. `testProduceEmptyInput()` - Handle empty input

**Test Approach:**
- Create test topics using TopicService
- Produce messages using ProduceCommand
- Verify messages using KafkaConsumer
- Use temporary files for file-based tests
- Validate output messages and counts

### ConsumeCommand Tests

**Test Coverage (14 tests):**

1. `testConsumeStandalone()` - Default standalone mode
2. `testConsumeWithGroup()` - Consumer group mode
3. `testConsumeFromBeginning()` - Offset control
4. `testConsumeFromEnd()` - Offset control
5. `testConsumeSpecificOffset()` - Offset + partition
6. `testConsumeSpecificPartition()` - Partition filtering
7. `testConsumeMaxMessages()` - Message limit
8. `testConsumeTableFormat()` - Table output
9. `testConsumeJsonFormat()` - JSON output
10. `testConsumeYamlFormat()` - YAML output
11. `testConsumeValueFormat()` - Value-only output
12. `testConsumeNoMessages()` - Empty topic
13. `testConsumeNonExistentTopic()` - Error handling
14. `testConsumeNoContext()` - Context validation

**Test Approach:**
- Create test topics using TopicService
- Produce known messages using KafkaProducer
- Consume using ConsumeCommand
- Verify output contains expected messages
- Test all output formats

### Native Integration Tests

- `ProduceCommandIT` - Extends ProduceCommandTest, runs against native executable
- `ConsumeCommandIT` - Extends ConsumeCommandTest, runs against native executable

## Integration with Context Management

Both commands integrate with the context system:

**Context Resolution:**

1. Load current context from root config
2. If no current context, return error with guidance
3. Merge context config using `common` + client type section (`producer` or `consumer`)
4. Create Kafka client with merged configuration

**Example with contexts:**

```bash
# Set production context
clik context use prod

# Produce messages to prod cluster
echo "Production message" | clik produce important-topic

# Consume from prod cluster
clik consume important-topic --from-end --max-messages 10

# Switch to dev and test
clik context use dev
clik produce test-topic --file test-messages.txt
clik consume test-topic --from-beginning
```

## Error Messages & User Experience

### Error Message Guidelines

1. **Clear and actionable**
   ```
   Error: File not found: /path/to/file.txt
   ```

2. **Provide context**
   ```
   Error: No current context set. Use 'clik context use <name>' to set a context.
   ```

3. **Suggest fixes**
   ```
   Error: --from-offset requires --partition to be specified
   ```

### Success Messages

**Produce:**
```
10 messages sent successfully
```

**Consume (end of one-time consumption):**
```
No messages consumed
```

**Consume (continuous mode interrupted):**
```
15 messages consumed
```

## Implementation Phases

### Phase 1: Foundation ✅ COMPLETED
- [x] Update KafkaClientFactory with createProducer() and createConsumer()
- [x] Create ConsumedMessage model with builder pattern

### Phase 2: Produce Command ✅ COMPLETED
- [x] Implement ProduceCommand with file/stdin/interactive support
- [x] Add to Clik.java subcommands
- [x] Write ProduceCommandTest (8 tests)
- [x] Write ProduceCommandIT for native builds

### Phase 3: Consume Command ✅ COMPLETED
- [x] Implement ConsumeCommand with standalone/group modes
- [x] Support offset control options
- [x] Implement multiple output formats (table, JSON, YAML, value)
- [x] Add to Clik.java subcommands
- [x] Write ConsumeCommandTest (14 tests)
- [x] Write ConsumeCommandIT for native builds

### Phase 4: Testing & Documentation ✅ COMPLETED
- [x] All tests passing (22 tests total)
- [x] Native image reflection configuration verified
- [x] Specification document (this file)
- [x] CLAUDE.md updates

### Phase 5: Future Enhancements
- [ ] Custom serializers/deserializers (Avro, Protobuf)
- [ ] Message headers support
- [ ] Advanced filtering and transformation
- [ ] Transactional producer support
- [ ] Consumer offset commit control

## Key Design Decisions

1. **String-only serialization**: Simple for MVP, easy to add other formats later
2. **Top-level commands**: `clik produce` and `clik consume` for discoverability
3. **Standalone consumer default**: Better for inspection/debugging (doesn't affect consumer groups)
4. **Table output default**: Best readability in terminal
5. **Shutdown hook for --follow**: Clean Ctrl+C handling
6. **File-based testing**: stdin manipulation doesn't work with QuarkusMainTest
7. **No offset commits in consume**: Read-only operation to avoid side effects
8. **Generated group IDs**: Prevents accidental consumer group creation

## Success Criteria

- ✅ Can produce messages from file and stdin
- ✅ Can consume messages in one-time and continuous modes
- ✅ Supports all output formats (table, JSON, YAML, value)
- ✅ Proper offset control for consumers
- ✅ Consumer group support with --group flag
- ✅ All tests passing (22 tests: 8 produce + 14 consume)
- ✅ Native image builds successfully
- ✅ Documentation complete

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Producer API](https://kafka.apache.org/documentation/#producerapi)
- [Kafka Consumer API](https://kafka.apache.org/documentation/#consumerapi)
- [Kafka Clients Configuration](https://kafka.apache.org/documentation/#configuration)

## Appendices

### Appendix A: Common Use Cases

**Quick message inspection:**
```bash
# View last 10 messages in a topic
clik consume my-topic --from-end --max-messages 10

# View first 10 messages
clik consume my-topic --from-beginning --max-messages 10
```

**Debugging production issues:**
```bash
# Consume from specific offset where issue occurred
clik consume my-topic --partition 5 --from-offset 12345 --max-messages 20 -o json
```

**Load testing:**
```bash
# Generate test messages
for i in {1..1000}; do echo "Test message $i"; done > messages.txt
clik produce my-topic --file messages.txt --key test-run-1
```

**Data migration verification:**
```bash
# Export messages from one topic
clik consume source-topic --from-beginning -o value > exported.txt

# Import to another topic
clik produce target-topic --file exported.txt
```

### Appendix B: Offset Control Examples

**From beginning:**
```bash
# Consume all messages
clik consume my-topic --from-beginning
```

**From end (latest):**
```bash
# Only consume new messages
clik consume my-topic --from-end --follow
```

**From specific offset:**
```bash
# Start from offset 1000 in partition 0
clik consume my-topic --partition 0 --from-offset 1000
```

**Continuous monitoring:**
```bash
# Monitor topic for new messages (like tail -f)
clik consume my-topic --follow
```
