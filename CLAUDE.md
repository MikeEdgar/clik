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
  - `config/` - Configuration services (ContextService, ConfigurationLoader, ContextValidator)
  - `version/` - Application version provider
- `src/main/resources/` - Configuration files and resources
- `src/test/java/` - Test code
- `src/test/resources/` - Test fixtures and resources
- `specs/` - Feature specifications (CONTEXT.md)

### Main Entry Point

`Clik.java` (`src/main/java/io/streamshub/clik/Clik.java`) is the top-level Picocli command annotated with `@TopCommand`. It defines:
- The CLI application name: "clik"
- Built-in help and version options
- Subcommands structure (context management commands)
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

### Configuration

`application.properties` (`src/main/resources/application.properties`) configures:
- Quarkus banner disabled
- Dev services disabled
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
- Logs are redirected to stderr in tests (WARN level) to keep stdout clean for command output
- Use temporary directories for all file-based tests to avoid conflicts
- Always clean up test contexts after each test to ensure isolation

### Native Image Considerations

When building native images with GraalVM:
- Use the `native` Maven profile: `./mvnw package -Pnative`
- Reflection, resources, and dynamic features may require registration
- Quarkus handles most Kafka client native image configuration automatically

## Roadmap

See `specs/CONTEXT.md` for detailed implementation phases. Current status:

**Phase 1: Core Context Management (✅ COMPLETED)**
- All basic context commands implemented and tested
- XDG Base Directory specification compliance
- Multiple output formats (table, yaml, json, name, properties)
- Comprehensive test coverage (63 tests passing)

**Phase 2: Enhanced Context Features (IN PROGRESS)**
- Integration tests completed
- Context rename command completed
- Shell completion for context names (pending)
- Context update command (pending)

**Phase 3: Advanced Features (FUTURE)**
- Credential helpers/plugins
- Environment variable substitution
- Context templates
- Context export/import
- Context namespaces/projects
