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
- `src/main/resources/` - Configuration files and resources
- `src/test/java/` - Test code

### Main Entry Point

`Clik.java` (`src/main/java/io/streamshub/clik/Clik.java`) is the top-level Picocli command annotated with `@TopCommand`. It defines:
- The CLI application name: "clik"
- Built-in help and version options
- Subcommands structure (currently only help command registered)
- Version provider that pulls version from `quarkus.application.version` config property

### Configuration

`application.properties` (`src/main/resources/application.properties`) configures:
- Quarkus banner disabled
- Dev services disabled
- Log levels by profile: WARN (prod), INFO (dev/test)

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

### Native Image Considerations

When building native images with GraalVM:
- Use the `native` Maven profile: `./mvnw package -Pnative`
- Reflection, resources, and dynamic features may require registration
- Quarkus handles most Kafka client native image configuration automatically
