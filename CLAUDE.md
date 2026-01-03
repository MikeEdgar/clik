# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Clik is a command-line interface (CLI) for Apache Kafka built with Quarkus and Picocli. The project uses Quarkus 3.30.4 with Java 21 and Kafka clients 4.1.1.

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
./target/clik-0.1.0-SNAPSHOT-runner
```

## Architecture

### Technology Stack

- **Framework**: Quarkus 3.30.4 (Jakarta EE with CDI dependency injection)
- **CLI Framework**: Picocli (integrated via quarkus-picocli)
- **Kafka Client**: Apache Kafka 4.1.1
- **Data Formats**: JSON-P (Jakarta JSON Processing), SnakeYAML, yaml-json converter
- **Utilities**: ascii-table for tabular output
- **Build Tool**: Maven with wrapper

### Package Structure

The codebase follows a standard Maven project layout:
- `src/main/java/io/streamshub/clik/` - Main application code
  - `command/context/` - Context management commands
  - `command/topic/` - Topic management commands
  - `command/group/` - Consumer group management commands
  - `config/` - Configuration services (ContextService, ConfigurationLoader, ContextValidator)
  - `kafka/` - Kafka services (KafkaClientFactory, TopicService, GroupService)
  - `kafka/model/` - Data models (TopicInfo, GroupInfo, etc.)
  - `version/` - Application version provider
- `src/main/resources/` - Configuration files and resources
- `src/test/java/` - Test code
- `src/test/resources/` - Test fixtures and resources
- `specs/` - Feature specifications (CONTEXT.md, TOPIC.md, GROUP.md)

### Main Entry Point

`Clik.java` (`src/main/java/io/streamshub/clik/Clik.java`) is the top-level Picocli command annotated with `@TopCommand`. It defines:
- The CLI application name: "clik"
- Built-in help and version options
- Subcommands structure (context, topic, and group management commands)
- Version provider that pulls version from `quarkus.application.version` config property

### Implemented Features

#### Context Management

Clik implements a kubectl-like context management system for Kafka clusters. See `specs/CONTEXT.md` for the full specification.

**Implemented Commands:**
- `clik context create <name>` - Create a new context with Kafka configuration
- `clik context list` - List all available contexts (supports table, yaml, json, name formats)
- `clik context use <name>` - Switch to a different context
- `clik context current` - Display the current active context
- `clik context delete <name>` - Delete a context
- `clik context show <name>` - Display detailed configuration for a context
- `clik context rename <old-name> <new-name>` - Rename a context

**Configuration Storage:**
- Contexts are stored in `$XDG_CONFIG_HOME/clik/contexts/<name>/config.yaml` (defaults to `~/.config/clik`)
- Follows XDG Base Directory Specification
- Configuration is organized by client type: common, admin, consumer, producer
- Current context is tracked in `$XDG_CONFIG_HOME/clik/config.yaml`

**Key Services:**
- `ContextService` - CRUD operations for contexts, manages context directories and files
- `ConfigurationLoader` - Loads and merges configurations, supports YAML and properties formats
- `ContextValidator` - Validates context names and configurations

#### Topic Management

Clik implements comprehensive Kafka topic management operations. See `specs/TOPIC.md` for the full specification.

**Implemented Commands:**
- `clik topic create <name>` - Create a new topic with custom partitions, replication, and configuration
- `clik topic list` - List all topics (supports table, yaml, json, name formats)
- `clik topic describe <name>` - Display detailed topic information including partition details
- `clik topic alter <name>` - Alter topic configuration and partition count
- `clik topic delete <name>` - Delete one or more topics (with confirmation prompt)

**Key Features:**
- Multiple output formats (table, yaml, json, name)
- Partition and replication factor configuration
- Topic-level configuration management (set/delete configs)
- Partition count increases (cannot decrease - Kafka limitation)
- Internal topic filtering
- Integration with context management

**Key Services:**
- `KafkaClientFactory` - Creates Kafka AdminClient from context configuration
- `TopicService` - CRUD operations for topics, configuration management, partition operations
- `TopicInfo` / `PartitionInfo` - Data models with @RegisterForReflection for native builds

#### Group Management

Clik implements Kafka consumer group monitoring and management. See `specs/GROUP.md` for the full specification.

**Implemented Commands:**
- `clik group list` - List all consumer groups with state and member count
- `clik group describe <groupId>` - Display detailed group information including member assignments and lag
- `clik group delete <groupId>` - Delete one or more consumer groups
- `clik group alter <groupId>` - Alter consumer group offsets or delete offsets from the group

**Key Features:**
- Support for all Kafka 4.1 group types: consumer, classic, share, streams
- Consumer lag monitoring and calculation
- Partition assignment visualization
- Multiple output formats (table, yaml, json, name)
- Group type filtering
- Member and coordinator information
- **Offset management** with multiple strategies:
  - Reset to earliest/latest
  - Set to specific offset
  - Shift by relative amount
  - Reset to timestamp or duration
  - Delete offsets from group
- Flexible topic:partition syntax for offset targeting
- Integration with context management

**Key Services:**
- `GroupService` - Operations for listing, describing, deleting groups, offset management, and lag calculation
- `GroupInfo` / `GroupMemberInfo` / `CoordinatorInfo` / `OffsetLagInfo` - Data models with @RegisterForReflection

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

Current implementation status across all feature areas:

### Context Management (✅ COMPLETED)

See `specs/CONTEXT.md` for detailed specification.

**Phase 1: Core Context Management (✅ COMPLETED)**
- All basic context commands implemented and tested
- XDG Base Directory specification compliance
- Multiple output formats (table, yaml, json, name, properties)
- Comprehensive test coverage (29 integration tests in ContextCommandTest, ContextCommandIT)

**Phase 2: Enhanced Context Features (✅ COMPLETED)**
- Integration tests completed
- Context rename command completed

**Future Enhancements:**
- Shell completion for context names
- Context update command
- Credential helpers/plugins
- Environment variable substitution
- Context templates and namespaces

### Topic Management (✅ COMPLETED)

See `specs/TOPIC.md` for detailed specification.

**Phase 1: Core Topic Operations (✅ COMPLETED)**
- All CRUD operations implemented: create, list, describe, alter, delete
- Configuration management (set/delete topic configs)
- Partition count increases
- Multiple output formats (table, yaml, json, name)
- Integration with context management
- Comprehensive test coverage (27 integration tests in TopicCommandTest, TopicCommandIT)
- Unit tests for services (13 tests in TopicServiceTest, 4 tests in KafkaClientFactoryTest)

**Future Enhancements:**
- Topic templates/presets
- Bulk operations from file
- Topic cloning/copying
- Topic metrics/statistics

### Group Management (✅ COMPLETED)

See `specs/GROUP.md` for detailed specification.

**Phase 1: Core Group Operations (✅ COMPLETED)**
- List and describe operations implemented
- Support for all Kafka 4.1 group types (consumer, classic, share, streams)
- Consumer lag monitoring and calculation
- Multiple output formats (table, yaml, json, name)
- Integration with context management
- Comprehensive test coverage (27 integration tests in GroupCommandTest, GroupCommandIT)
- Unit tests for services (9 tests in GroupServiceTest)

**Phase 2: Advanced Group Management (✅ COMPLETED)**
- ✅ Consumer group deletion (delete command)
- ✅ Offset management (alter command with multiple strategies)
  - Reset to earliest/latest offsets
  - Set specific offsets
  - Shift offsets by relative amount
  - Reset to timestamp or duration
  - Delete offsets from group
  - Topic:partition targeting syntax

**Future Enhancements:**
- Consumer group quota management
- Real-time lag monitoring
- Group rebalancing controls
- Consumer group export/import

### Overall Test Coverage

**Total Tests: 149 passing**
- Unit tests: 60 tests (ContextService, ConfigurationLoader, ContextValidator, TopicService, GroupService, KafkaClientFactory)
- Integration tests: 89 tests (29 ContextCommandTest + 27 TopicCommandTest + 27 GroupCommandTest + native IT variants)
- All tests passing in both JVM and native modes
