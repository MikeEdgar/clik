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
| `-v, --value <value>` | Single message value (mutually exclusive with file/interactive) | - |
| `-k, --key <key>` | Message key (applied to all messages, not compatible with --input) | null |
| `-p, --partition <num>` | Target partition number (not compatible with --input) | - |
| `--header <key=value>` | Message header (repeatable, supports duplicate keys, not compatible with --input) | - |
| `-t, --timestamp <ts>` | Message timestamp in epoch milliseconds or ISO-8601 format (not compatible with --input) | - |
| `-I, --input <format>` | Format string for parsing input lines (e.g., '%k %v' for key-value pairs) | - |

**Examples:**

```bash
# Produce messages from a file
clik produce my-topic --file messages.txt

# Produce messages from stdin (via pipe)
echo -e "msg1\nmsg2\nmsg3" | clik produce my-topic

# Produce a single message
clik produce my-topic --value "Hello World"

# Produce with a specific key
clik produce my-topic --file messages.txt --key user123

# Produce to a specific partition
clik produce my-topic --file messages.txt --partition 2

# Produce with headers
clik produce my-topic --value "test" --header "content-type=application/json" --header "version=1.0"

# Produce with timestamp (epoch milliseconds or ISO-8601)
clik produce my-topic --value "timestamped message" --timestamp 1735401600000
clik produce my-topic --value "timestamped message" --timestamp "2026-01-04T12:00:00Z"

# Produce with binary encoding (base64 or hex)
clik produce my-topic --value "base64:SGVsbG8gV29ybGQ=" --key "hex:6b6579"
clik produce my-topic --value "test" --header "signature=base64:U2lnbmF0dXJl"

# Interactive mode
clik produce my-topic --interactive

# Format string for structured input (key-value pairs)
echo -e "key1 value1\nkey2 value2" | clik produce my-topic --input "%k %v"

# Format string with base64-encoded key
cat data.txt | clik produce my-topic --input "%{base64:k} %v"

# Format string with headers and timestamp
cat data.txt | clik produce my-topic --input "%k %v %{h[type]} %T"

# Format string with tab delimiter (using unicode escape)
cat data.tsv | clik produce my-topic --input "%k\u0009%v"

# Format string with all fields
cat data.txt | clik produce my-topic --input "%k %v %{h[content-type]} %T %p"
```

**Output:**
```
3 messages sent successfully
```

**Behavior:**

1. Load configuration from current context
2. Create KafkaProducer with String serializer
3. Read messages from input source (file, stdin, interactive, or single value)
4. Parse input according to format string if `--input` is specified
5. Decode binary-encoded values (base64:, hex:) in keys, values, and headers
6. Send each message as a ProducerRecord with appropriate metadata
7. Flush producer to ensure all messages are sent
8. Report success/failure counts

**Input Sources Priority:**
1. If `--value` specified: send single message
2. Else if `--file` specified: read from file
3. Else if `--interactive` specified: read from System.in with prompt
4. Else: read from stdin (piped input)

**Binary Encoding:**
- Supports `base64:` and `hex:` prefixes for keys, values, and header values
- Example: `--key "base64:dGVzdC1rZXk="` decodes to "test-key"
- Example: `--value "hex:48656c6c6f"` decodes to "Hello"
- Plain text (no prefix) is treated as UTF-8

**Format String Syntax:**
- `%k` - Message key (plain text)
- `%v` - Message value (plain text)
- `%h` - Generic header (matches any header, parses as key=value)
- `%T` - Timestamp (epoch milliseconds)
- `%p` - Partition number
- `%%` - Literal percent character
- `%{base64:k}` - Base64-encoded key
- `%{hex:v}` - Hex-encoded value
- `%{h[name]}` - Named header (matches specific header name)
- `%{base64:h[signature]}` - Base64-encoded named header
- `\uXXXX` - Unicode character (e.g., `\u0009` for tab)

**Important Notes on %h (Generic Header):**
- In producer INPUT parsing, `%h` matches exactly ONE header as `key=value`
- This is different from consumer OUTPUT where `%h` outputs ALL headers
- To parse multiple headers, use multiple placeholders: `%h %h %h`
- Or use named placeholders: `%{h[type]} %{h[version]} %{h[encoding]}`
- This asymmetry exists because input parsing requires delimiters between placeholders

**Important Notes:**
- Input modes (`--value`, `--file`, `--interactive`, stdin) are mutually exclusive
- `--input` format string cannot be used with `--value`
- `--input` format string cannot be used with global `--key`, `--header`, `--timestamp`, or `--partition` options
- Each line in the input is treated as a separate message (except with `--value`)
- Empty lines produce empty messages
- Messages are sent synchronously for reliability
- All messages use the same metadata if global options are specified (without `--input`)
- Format strings must contain at least one placeholder
- Duplicate headers are allowed (Kafka supports multiple headers with the same key)
  - Example: `%{h.tag} %{h.tag} %{h.tag}` can parse three separate "tag" headers from input
  - Both named headers and generic `%h` placeholders can appear multiple times

**Error Conditions:**
- File not found
- No current context set
- Multiple input modes specified (--value, --file, --interactive)
- `--input` used with `--value`
- `--input` used with global `--key`, `--header`, `--timestamp`, or `--partition`
- Invalid format string (missing placeholders, duplicate headers, etc.)
- Invalid binary encoding (malformed base64 or hex)
- Format string parsing errors (missing delimiters, invalid header format, etc.)
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
| `-o, --output <format>` | Output format: table, json, yaml, or custom format string | table |
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

# Output values only (no metadata) using custom format
clik consume my-topic --from-beginning -o "%v"

# Custom format string (partition:offset key=value)
clik consume my-topic --from-beginning -o "%p:%o %k=%v"

# CSV format
clik consume my-topic --from-beginning -o '"%k","%v",%p,%o,%T'

# JSON-like custom format
clik consume my-topic --from-beginning -o '{"key":"%k","value":"%v","partition":%p}'

# Tab-separated values
clik consume my-topic --from-beginning -o "%k\u0009%v\u0009%T"
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

**Output (custom format string `%v` - values only):**
```
Hello World
Test message
Another message
```

**Output (custom format string `%p:%o %k=%v`):**
```
0:42 user123=Hello World
0:43 =Test message
1:28 user456=Another message
```

**Output (CSV format `"%k","%v",%p,%o,%T`):**
```
"user123","Hello World",0,42,1704326400000
"","Test message",0,43,1704326401000
"user456","Another message",1,28,1704326385000
```

### Consumer Format String Syntax

Format strings allow custom output formatting for consumed messages. The syntax is similar to the producer's `--input` option but for **output** instead of input.

**Important:** For output formatting, encoding prefixes (like `base64:` or `hex:`) **encode** the data before printing, which is the opposite of input parsing where they decode.

**Supported Placeholders:**

| Placeholder | Description | Example Output |
|-------------|-------------|----------------|
| `%k` | Message key (UTF-8 string) | `user123` |
| `%v` | Message value (UTF-8 string) | `Hello World` |
| `%o` | Offset | `42` |
| `%p` | Partition number | `0` |
| `%T` | Timestamp (epoch milliseconds) | `1704326400000` |
| `%h` | All remaining headers as `key=value` pairs (excludes headers already output) | `type=json version=1.0` |
| `%{h[name]}` | All headers with this name (space-separated if multiple, marks as output) | `tag=v1 tag=v2` |
| `%{base64:k}` | Base64-encoded key | `dGVzdC1rZXk=` |
| `%{hex:v}` | Hex-encoded value | `48656c6c6f` |
| `%{base64:h[sig]}` | Base64-encoded header | `sig=U2lnbmF0dXJl` |
| `%%` | Literal percent character | `%` |
| `\uXXXX` | Unicode character (e.g., `\u0009` for tab) | `\t` |

**Header Output Behavior:**

Output formatting maintains state to prevent duplicate header output:

1. **Named headers** (`%{h[name]}`): Outputs ALL headers with the specified name
   - Multiple values appear as space-separated `name=value` pairs
   - Example: `%{h[tag]}` with two "tag" headers outputs: `tag=v1 tag=v2`
   - After output, these headers are marked and excluded from subsequent `%h`

2. **Generic headers** (`%h`): Outputs all headers NOT yet output by named placeholders
   - Only outputs headers that haven't been output earlier in the format string
   - Can appear multiple times; each outputs remaining headers
   - Example format `%{h[type]} %h` on record with headers `type=json, version=1.0, encoding=utf8`:
     - First placeholder outputs: `type=json`
     - Second placeholder outputs: `version=1.0 encoding=utf8`

3. **Encoding support**: Both named and generic headers support encoding
   - `%{base64:h[signature]}` - Base64-encode the signature header value
   - `%{hex:h}` - Hex-encode all remaining header values

**Examples:**
```bash
# Output specific header followed by remaining headers
clik consume my-topic -o "%v | priority: %{h[priority]} | other: %h"

# Output multiple headers with same name
clik consume my-topic -o "%v tags=[%{h[tag]}]"
# If record has tag=important, tag=urgent: "value tags=[tag=important tag=urgent]"

# Prevent duplicate header output
clik consume my-topic -o "%{h[type]} %h"
# type header appears only once (in first placeholder, not in %h)
```

**Format String Examples:**

```bash
# Simple key-value output
clik consume my-topic --from-beginning -o "%k=%v"

# With partition and offset
clik consume my-topic --from-beginning -o "%p:%o %k %v"

# Tab-separated values
clik consume my-topic --from-beginning -o "%k\u0009%v"

# CSV with timestamp
clik consume my-topic --from-beginning -o '"%k","%v",%T'

# JSON-like format
clik consume my-topic --from-beginning -o '{"key":"%k","value":"%v"}'

# With headers
clik consume my-topic --from-beginning -o "%v [%{h[content-type]}]"

# Base64 encode binary values
clik consume my-topic --from-beginning -o "%{base64:k} %{base64:v}"

# Hex encode for debugging
clik consume my-topic --from-beginning -o "%k = %{hex:v}"
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
- Invalid output format (unknown predefined format or invalid format string)
- Format string validation errors:
  - No placeholders in format string
  - Unknown placeholder (e.g., `%x`)
  - Unclosed placeholder (e.g., `%{base64:k`)
  - Invalid encoding type
  - Empty header name (e.g., `%{h[]}`)
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
├── support/
│   ├── Encoding.java                     # Binary encoding utilities (base64, hex)
│   ├── FormatStringParser.java           # Shared format string parser (base class)
│   ├── InputParser.java                  # Producer input line parser
│   ├── OutputFormatter.java              # Consumer output formatter
│   ├── FormatToken.java                  # Sealed interface for format tokens
│   ├── LiteralToken.java                 # Literal text in format strings
│   ├── PlaceholderToken.java             # Placeholder token (with type, encoding, name)
│   └── PlaceholderType.java              # Enum: KEY, VALUE, HEADER, TIMESTAMP, PARTITION, OFFSET
└── Clik.java                             # Updated with produce/consume subcommands
```

### KafkaRecord Model

The `KafkaRecord` class is an immutable record representing Kafka message components:

```java
public record KafkaRecord(
    ByteArray key,
    ByteArray value,
    List<Header> headers,
    Long timestamp,
    Integer partition,
    Long offset
) {
    // Nested immutable byte array wrapper
    public record ByteArray(byte[] bytes) { }

    // Header record
    public record Header(String key, ByteArray value) { }

    // Convenience methods
    public String keyString(String defaultValue);
    public String valueString(String defaultValue);
    public Header firstHeader(String key);         // First header with key
    public List<Header> headers(String key);       // All headers with key

    // Builder for construction
    public static Builder builder() { }
}
```

**Key features**:
- Immutable design with defensive copying of byte arrays
- Supports multiple headers with same key (Kafka allows duplicates)
- Null-safe convenience methods for string conversion
- Builder pattern for flexible construction
- Used by both InputParser (producer) and OutputFormatter (consumer)

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

**Test Coverage (54 passing tests, 1 skipped):**

**Basic Production (8 tests):**
1. `testProduceFromStdin()` - Produce messages from stdin (disabled for QuarkusMainTest)
2. `testProduceFromFile()` - Produce messages from file
3. `testProduceWithKey()` - Verify key assignment
4. `testProduceWithPartition()` - Verify partition targeting
5. `testProduceNoContext()` - Error when no context
6. `testProduceFileNotFound()` - Error handling
7. `testProduceMultipleMessages()` - Batch sending (10 messages)
8. `testProduceEmptyInput()` - Handle empty input

**Single Value & Headers (11 tests):**
9. `testProduceWithValue()` - Single message via --value
10. `testProduceWithValueAndKey()` - Value with key
11. `testProduceWithValueAndPartition()` - Value with partition
12. `testProduceWithSingleHeader()` - Single header
13. `testProduceWithMultipleHeaders()` - Multiple headers
14. `testProduceWithDuplicateHeaderKeys()` - Duplicate header keys
15. `testProduceWithHeadersFromFile()` - Headers applied to file input
16. `testProduceWithTimestampEpochMillis()` - Timestamp as epoch milliseconds
17. `testProduceWithTimestampISO8601()` - Timestamp as ISO-8601
18. `testProduceWithValueHeadersAndTimestamp()` - Combined metadata
19. `testProduceHeaderWithEqualsInValue()` - Header value contains =

**Binary Encoding (10 tests):**
20. `testProduceWithBase64Value()` - Base64-encoded value
21. `testProduceWithHexValue()` - Hex-encoded value
22. `testProduceWithBase64Key()` - Base64-encoded key
23. `testProduceWithHexKey()` - Hex-encoded key
24. `testProduceWithBase64Header()` - Base64-encoded header value
25. `testProduceWithHexHeader()` - Hex-encoded header value
26. `testProduceWithMixedEncodings()` - Mixed encoding types
27. `testProduceInvalidBase64()` - Invalid base64 error
28. `testProduceInvalidHexOddLength()` - Hex with odd length error
29. `testProduceInvalidHexCharacters()` - Invalid hex characters error

**Format String (17 tests):**
30. `testProduceWithInputFormatKeyValue()` - Format: %k %v
31. `testProduceWithInputFormatBase64Key()` - Format: %{base64:k} %v
32. `testProduceWithInputFormatNamedHeader()` - Format: %k %v %{h[name]}
33. `testProduceWithInputFormatGenericHeader()` - Format: %k %v %h
34. `testProduceWithInputFormatTimestamp()` - Format: %k %v %T
35. `testProduceWithInputFormatPartition()` - Format: %k %v %p
36. `testProduceWithInputFormatMixedEncodings()` - Format with mixed encodings
37. `testProduceWithInputFormatUnicodeDelimiter()` - Format: %k\u0009%v (tab)
38. `testProduceWithInputFormatValueWithSpaces()` - Values containing spaces
39. `testProduceWithInputFormatMultipleHeaders()` - Multiple named headers
40. `testProduceWithInputFormatAllFields()` - All fields in one format
41. `testProduceInputFormatConflictWithValue()` - Error: --input with --value
42. `testProduceInputFormatConflictWithKey()` - Error: --input with --key
43. `testProduceInputFormatConflictWithHeader()` - Error: --input with --header
44. `testProduceInputFormatConflictWithTimestamp()` - Error: --input with --timestamp
45. `testProduceInputFormatConflictWithPartition()` - Error: --input with --partition
46. `testProduceInputFormatInvalidFormatString()` - Invalid format string error

**Error Cases (9 tests):**
47. `testProduceMutualExclusivityValueAndFile()` - Error handling
48. `testProduceMutualExclusivityValueAndInteractive()` - Error handling
49. `testProduceInvalidHeaderFormat()` - Invalid header error
50. `testProduceInvalidHeaderFormatEmptyKey()` - Empty header key error
51. `testProduceInvalidTimestampFormat()` - Invalid timestamp error
52. `testProduceEmptyValue()` - Handle empty value
53. `testProduceInputFormatMissingDelimiter()` - Missing delimiter error
54. `testProduceInputFormatInvalidHeaderFormat()` - Invalid header in format
55. `testProduceBackwardsCompatiblePlainText()` - Plain text compatibility

**Test Approach:**
- Create test topics using TopicService
- Produce messages using ProduceCommand
- Verify messages using KafkaConsumer
- Use temporary files for file-based tests
- Validate output messages, counts, and metadata
- Test all encoding types (plain, base64, hex)
- Test format string parsing and validation
- Test error conditions and validation

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
- [x] All tests passing (22 tests: 8 produce + 14 consume)
- [x] Native image reflection configuration verified
- [x] Specification document (this file)
- [x] CLAUDE.md updates

### Phase 5: Binary Encoding & Headers ✅ COMPLETED
- [x] Add base64 and hex encoding support for keys, values, and headers
- [x] Add --header option for message headers
- [x] Add --timestamp option for message timestamps
- [x] Add --value option for single message production
- [x] Add comprehensive encoding tests (26 EncodingTest unit tests + 10 integration tests)
- [x] Add header and timestamp tests (11 integration tests)

### Phase 6: Format String Support ✅ COMPLETED
- [x] Design and implement format string parser
- [x] Add --input option for structured input parsing
- [x] Support simple placeholders (%k, %v, %h, %T, %p, %%)
- [x] Support parameterized placeholders (%{base64:k}, %{hex:v}, %{h[name]})
- [x] Support unicode escapes (\uXXXX)
- [x] Add format string validation and error handling
- [x] Add FormatParser unit tests (35 tests)
- [x] Add format string integration tests (20 tests)
- [x] Total producer tests: 54 passing (1 skipped)

### Phase 7: Future Enhancements
- [ ] Custom serializers/deserializers (Avro, Protobuf)
- [ ] Advanced filtering and transformation
- [ ] Transactional producer support
- [ ] Consumer offset commit control
- [ ] Consumer format string support for output formatting

## Key Design Decisions

1. **String-only serialization**: Simple for MVP, easy to add other formats later
2. **Top-level commands**: `clik produce` and `clik consume` for discoverability
3. **Standalone consumer default**: Better for inspection/debugging (doesn't affect consumer groups)
4. **Table output default**: Best readability in terminal
5. **Shutdown hook for --follow**: Clean Ctrl+C handling
6. **File-based testing**: stdin manipulation doesn't work with QuarkusMainTest
7. **No offset commits in consume**: Read-only operation to avoid side effects
8. **Generated group IDs**: Prevents accidental consumer group creation
9. **Prefix-based encoding**: `base64:` and `hex:` prefixes are intuitive and easy to parse
10. **Format string syntax**: Inspired by kcat/kafkacat for familiarity
11. **Sealed interfaces for tokens**: Type-safe token hierarchy with pattern matching
12. **Unicode escapes in format strings**: Allows special delimiters like tabs without shell quoting issues
13. **Validation at parse time**: Format string errors caught early before processing input
14. **Mutually exclusive options**: Prevents ambiguous combinations (--input vs global options)

## Success Criteria

- ✅ Can produce messages from file, stdin, interactive mode, and single value
- ✅ Can produce with keys, partitions, headers, and timestamps
- ✅ Supports binary encoding (base64 and hex) for keys, values, and headers
- ✅ Can parse structured input with format strings
- ✅ Format strings support all placeholders and encodings
- ✅ Can consume messages in one-time and continuous modes
- ✅ Supports all output formats (table, JSON, YAML, value)
- ✅ Proper offset control for consumers
- ✅ Consumer group support with --group flag
- ✅ All tests passing (286 tests: 54 produce + 14 consume + 26 Encoding + 35 FormatParser + other unit/integration tests)
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
# Export messages from one topic (values only)
clik consume source-topic --from-beginning -o "%v" > exported.txt

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

### Appendix C: Format String Use Cases

**Data export to CSV:**
```bash
# Export to CSV file
clik consume my-topic --from-beginning -o '"%k","%v",%p,%o,%T' > export.csv
```

**Log-style output:**
```bash
# Timestamp key: value format
clik consume my-topic --from-beginning -o "[%T] %k: %v"
```

**Debugging with partition and offset:**
```bash
# Show partition and offset with each message
clik consume my-topic --from-beginning -o "%p:%o %v"
```

**JSON Lines format:**
```bash
# Each message as JSON object (JSONL)
clik consume my-topic --from-beginning -o '{"key":"%k","value":"%v","ts":%T}' > data.jsonl
```

**Binary data inspection:**
```bash
# Hex dump of message values
clik consume my-topic --from-beginning -o "%k = %{hex:v}"

# Base64 encoded for safe transmission
clik consume my-topic --from-beginning -o "%{base64:k} %{base64:v}"
```

**Header extraction:**
```bash
# Show message with specific header
clik consume my-topic --from-beginning -o "%v (type: %{h[content-type]})"

# Show all headers
clik consume my-topic --from-beginning -o "%v | headers: %h"
```
