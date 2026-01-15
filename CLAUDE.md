# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Clik is a command-line interface (CLI) for Apache Kafka built with Quarkus and Picocli. The project uses Quarkus, Java, and Kafka clients.

## Build and Development Commands

### Building the Application

```bash
# Standard JVM build
./mvnw clean package

# Build native executable (requires GraalVM)
./mvnw clean package -Pnative

# Run in development mode with hot reload
./mvnw quarkus:dev
```

### Running Tests

```bash
# Run all tests
./mvnw test

# Run a single test class
./mvnw test -Dtest=ClassName

# Run a specific test method
./mvnw test -Dtest=ClassName#methodName

# Native image integration tests
./mvnw verify -Pnative
```

### Running the Application

```bash
# During development (JVM mode)
./mvnw quarkus:dev

# Run packaged JAR
java -jar target/quarkus-app/quarkus-run.jar

# Run native executable (after native build)
./target/clik-*-runner
```

## Architecture

### Technology Stack

- **Framework**: Quarkus (Jakarta EE with CDI dependency injection)
- **CLI Framework**: Picocli (integrated via quarkus-picocli)
- **Kafka Client**: Apache Kafka
- **Data Formats**: JSON, YAML (Jackson / SnakeYAML)
- **Utilities**: ascii-table for tabular output
- **Build Tool**: Maven with wrapper

### Package Structure

The codebase follows a standard Maven project layout:
- `src/main/java/io/streamshub/clik/` - Main application code
  - `command/context/` - Context management commands
  - `command/topic/` - Topic management commands
  - `command/group/` - Consumer group management commands
  - `command/cluster/` - Cluster management commands
  - `command/acl/` - ACL management commands
  - `command/produce/` - Message producer command
  - `command/consume/` - Message consumer command
  - `config/` - Configuration services (ContextService, ConfigurationLoader, ContextValidator)
  - `kafka/` - Kafka services (KafkaClientFactory, TopicService, GroupService, ClusterService, AclService)
  - `kafka/model/` - Data models (TopicInfo, GroupInfo, ClusterInfo, AclInfo, ConsumedMessage, etc.)
  - `support/` - Utility classes (Encoding, FormatParser, format tokens, MessageComponents)
  - `version/` - Application version provider
- `src/main/resources/` - Configuration files and resources
- `src/test/java/` - Test code
- `src/test/resources/` - Test fixtures and resources
- `specs/` - Feature specifications (CONTEXT.md, TOPIC.md, GROUP.md, CLUSTER.md, ACL.md, PRODUCE_CONSUME.md)

### Main Entry Point

`Clik.java` (`src/main/java/io/streamshub/clik/Clik.java`) is the top-level Picocli command annotated with `@TopCommand`. It defines:
- The CLI application name: "clik"
- Built-in help and version options
- Subcommands structure (context, topic, and group management commands)
- Version provider that pulls version from `quarkus.application.version` config property

### Implemented Features

#### Context Management (✅ COMPLETED)
See `specs/CONTEXT.md` for complete specification.
- Commands: create, list, use, current, delete, describe, rename
- Key services: ContextService, ConfigurationLoader, ContextValidator
- Tests: 29 passing (integration)

#### Topic Management (✅ COMPLETED)
See `specs/TOPIC.md` for complete specification.
- Commands: create, list, describe, alter, delete
- Key services: TopicService, KafkaClientFactory
- Data models: TopicInfo, PartitionInfo
- Tests: 27 passing (integration) + 13 passing (service)

#### Group Management (✅ COMPLETED)
See `specs/GROUP.md` for complete specification.
- Commands: list, describe, delete, alter (with offset management strategies)
- Key services: GroupService
- Data models: GroupInfo, GroupMemberInfo, CoordinatorInfo, OffsetLagInfo
- Tests: 28 passing (integration) + 9 passing (service)

#### Cluster Management (✅ COMPLETED)
See `specs/CLUSTER.md` for complete specification.
- Commands: describe
- Key services: ClusterService (dual API call: describeCluster + describeMetadataQuorum)
- Data models: ClusterInfo, NodeInfo, QuorumInfo, ObserverState
- KRaft support: Role detection, quorum metadata, graceful ZooKeeper fallback
- Tests: 7 passing (integration) + 6 passing (service)

#### ACL Management (✅ COMPLETED)
See `specs/ACL.md` for complete specification.
- Commands: create, list, delete
- Key services: AclService
- Data models: AclInfo
- Resource-specific shortcuts (--topic, --group, --cluster, --transactional-id, --delegation-token, --user-resource)
- Option mixins: Resource.Options, Operation, Permission, PatternType
- Tests: 31 passing (integration)

#### Producer and Consumer (✅ COMPLETED)
See `specs/PRODUCE_CONSUME.md` for complete specification.
- Commands: produce, consume
- Key services: KafkaClientFactory (createProducer/createConsumer methods)
- Data models: ConsumedMessage
- Producer: Multiple input sources, format string support, binary encoding (base64/hex), headers, timestamps
- Consumer: Offset control, continuous mode, multiple output formats, partition filtering
- Tests: 54 passing (produce) + 14 passing (consume) + 61 passing (support)

### Configuration

`application.properties` (`src/main/resources/application.properties`) configures:
- Quarkus banner disabled
- **Kafka Dev Services**: Enabled for dev and test profiles, disabled in production
  - Uses Strimzi Kafka container via Testcontainers
  - Automatically starts a Kafka broker on demand during development and testing
  - No manual Kafka installation required for local development
- Log levels by profile: WARN (prod/test), INFO (dev)
- Production logs to stderr (`%prod.quarkus.log.console.stderr=true`)
- XDG_CONFIG_HOME environment variable support with fallback to `${user.home}/.config`

### Key Dependencies

- `quarkus-picocli`: CLI framework integration
- `quarkus-kafka-client`: Kafka connectivity
- `quarkus-jsonp`: JSON processing
- `snakeyaml` + `yaml-json`: YAML/JSON conversion
- `ascii-table`: Terminal table formatting

## Development Patterns

### Adding New Commands

New Picocli commands should be:
1. Created as classes implementing or annotated with `@CommandLine.Command`
2. Registered in the `subcommands` array of the `@TopCommand` annotation in `Clik.java`
3. Use CDI `@Inject` for dependency injection of Kafka clients or other services

### Quarkus Profiles

The application uses standard Quarkus profiles:
- `%dev` - Development mode (hot reload enabled)
- `%test` - Test execution
- `%prod` - Production builds

Use profile-specific configuration with `%profile.property=value` syntax in `application.properties`.

### Testing Patterns

#### Unit Tests
- Use standard JUnit 5 tests for services (ContextService, ConfigurationLoader, ContextValidator)
- Mock file system operations where appropriate
- Test configuration parsing, merging, and validation logic

#### Integration Tests
- Use `@QuarkusMainTest` for CLI integration tests that test full command execution
- Use `@QuarkusMainIntegrationTest` for testing packaged application
- Create temporary config directories using `@BeforeAll` to avoid conflicts between tests
- Override `xdg.config.home` via `QuarkusTestProfile.getConfigOverrides()` to isolate test contexts
- Clean up temporary directories in `@AfterEach` hooks

**Example Test Profile:**
```java
public static class TestConfig implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("xdg.config.home", tempConfigDir.toString());
    }
}
```

#### Test Fixtures
- Store expected output in `src/test/resources` for comparison tests
- Use Jackson ObjectMapper to parse and compare YAML/JSON outputs
- Use `result.getOutputStream()` for line-based assertions (Windows-compatible)

#### Important Test Considerations
- **Kafka Dev Services** automatically provides a Kafka broker for tests
  - Uses Testcontainers with Strimzi Kafka
  - First test run will download the container image
  - Subsequent runs reuse the cached image
  - No manual Kafka setup required for testing
- Logs are redirected to stderr in tests (WARN level) to keep stdout clean for command output
- Use temporary directories for all file-based tests to avoid conflicts
- Always clean up test contexts after each test to ensure isolation

### Native Image Considerations

When building native images with GraalVM:
- Use the `native` Maven profile: `./mvnw package -Pnative`
- Reflection, resources, and dynamic features may require registration
- Quarkus handles most Kafka client native image configuration automatically

### Git Commit Conventions

#### Commit Message Format

Standard commit messages should follow this structure:

```
<Short summary line>

<Detailed explanation of changes, including context and reasoning>

<Optional: Examples of usage or key changes>

Assisted-by: Claude Code (<model-version>) <noreply@anthropic.com>
Signed-off-by: <committer-name> <committer-email>
Co-authored-by: Claude <model-version> <noreply@anthropic.com>
```

**Important Guidelines:**
- **Summary line**: Concise description (50-72 characters preferred)
- **Body**: Detailed explanation of what changed and why
- **Omit test results**: Do not include test counts or pass/fail results in commit messages
- **Attribution format**: Always include the three attribution lines at the end with appropriate substitutions:
  - `<model-version>`: Current Claude model (e.g., "Sonnet 4.5", "Opus 4")
  - `<committer-name>`: Full name of the person creating the commit
  - `<committer-email>`: Email address of the committer

**Example:**

```
Implement group alter command for offset management

Add comprehensive `clik group alter` command that supports modifying
and deleting consumer group offsets with flexible topic:partition syntax.

Features:
- Six offset strategies: --to-earliest, --to-latest, --to-offset,
  --shift-by, --to-datetime, --by-duration
- Offset deletion via --delete flag
- Flexible topic:partition targeting (all/topic/specific partition)
- Confirmation prompt with --yes override
- Active member validation (prevents accidental changes)

Examples:
  clik group alter my-group --to-earliest ""
  clik group alter my-group --to-latest mytopic
  clik group alter my-group --shift-by -50:mytopic

Assisted-by: Claude Code (Sonnet 4.5) <noreply@anthropic.com>
Signed-off-by: Michael Edgar <medgar@redhat.com>
Co-authored-by: Claude Sonnet 4.5 <noreply@anthropic.com>
```

## Roadmap

Implementation status:

### Context Management (✅ COMPLETED)
Specification: `specs/CONTEXT.md`

### Topic Management (✅ COMPLETED)
Specification: `specs/TOPIC.md`

### Group Management (✅ COMPLETED)
Specification: `specs/GROUP.md`

### Cluster Management (✅ COMPLETED)
Specification: `specs/CLUSTER.md`

### ACL Management (✅ COMPLETED)
Specification: `specs/ACL.md`

### Producer and Consumer (✅ COMPLETED)
Specification: `specs/PRODUCE_CONSUME.md`

### Overall Test Coverage

**Total Tests: 389 passing (2 skipped)**
- Unit tests: 127 (ContextService, ConfigurationLoader, ContextValidator, TopicService, GroupService, ClusterService, KafkaClientFactory, Encoding, FormatParser, OutputFormatter)
- Integration tests: 262 (ContextCommandTest: 29, TopicCommandTest: 27, GroupCommandTest: 28, ClusterCommandTest: 7, AclCommandTest: 31, ProduceCommandTest: 54, ConsumeCommandTest: 14 + native IT variants)
- All tests passing in both JVM and native modes
