# Clik

A modern command-line interface for Apache Kafka, built with Quarkus and designed for developer productivity.

## Overview

Clik provides an intuitive, kubectl-like experience for managing Kafka clusters. It supports context-based configuration, comprehensive topic and consumer group management, and streamlined message production/consumption workflows.

**Built with:**
- Java
- Quarkus
- Apache Kafka Clients
- Picocli for command-line interface


## Quick Start

### Releases

Download the latest release for your OS [here](https://github.com/MikeEdgar/clik/releases).

### Bash Completion

After renaming the downloaded release executable to `clik` and placing it in a directory on your `PATH`, use one of the following to setup Bash completion.

Single-session

```shell
source <(clik completion generate)
```

Persist in user preferences

```shell
clik completion generate > ~/.local/share/bash-completion/clik
```

Persist globally

```shell
# Linux
clik completion generate > /etc/bash_completion.d/clik

# MacOS
clik completion generate > /usr/local/etc/bash_completion.d/clik
```

## Features

### 🔧 Context Management
Save and switch between multiple Kafka cluster configurations with kubectl-like context commands.

```bash
clik context create local --bootstrap-servers localhost:9092
clik context use local
```

See [specs/CONTEXT.md](specs/CONTEXT.md) for complete documentation.

### 📊 Topic Management
Create, list, describe, alter, and delete Kafka topics.

```bash
clik topic create my-topic --partitions 3
clik topic list
```

See [specs/TOPIC.md](specs/TOPIC.md) for complete documentation.

### 👥 Consumer Group Management
Monitor consumer lag, manage offsets, and control consumer group lifecycle.

```bash
clik group list
clik group describe my-consumer-group
```

See [specs/GROUP.md](specs/GROUP.md) for complete documentation.

### 🖥️ Cluster Management
View detailed cluster information including nodes, controllers, and KRaft quorum metadata.

```bash
clik cluster describe
```

See [specs/CLUSTER.md](specs/CLUSTER.md) for complete documentation.

### 🔒 ACL Management
Manage Kafka Access Control Lists (ACLs) to secure your cluster resources.

```bash
clik acl create --topic my-topic --operation READ --principal User:alice
clik acl list
```

See [specs/ACL.md](specs/ACL.md) for complete documentation.

### 📨 Message Production
Produce messages from files, stdin, or interactively with support for keys, headers, and timestamps.

```bash
echo "Hello Kafka" | clik produce my-topic
clik produce my-topic --file messages.txt
```

See [specs/PRODUCE_CONSUME.md](specs/PRODUCE_CONSUME.md) for complete documentation.

### 📥 Message Consumption
Consume messages with flexible offset control, output formatting, and continuous monitoring.

```bash
clik consume my-topic --from-beginning
clik consume my-topic --follow
```

See [specs/PRODUCE_CONSUME.md](specs/PRODUCE_CONSUME.md) for complete documentation.

## Build from Source

### Build and Run

**JVM mode** (requires Java 25+):
```bash
./mvnw clean package
java -jar target/quarkus-app/quarkus-run.jar --help
```

**Native executable** (requires GraalVM):
```bash
./mvnw clean package -Pnative
./target/clik-*-runner --help
```

### First Steps

1. **Create a context** for your Kafka cluster:
   ```bash
   clik context create local --bootstrap-servers localhost:9092
   ```

2. **Set it as current**:
   ```bash
   clik context use local
   ```

3. **Create a test topic**:
   ```bash
   clik topic create test-topic --partitions 1 --replication-factor 1
   ```

4. **Produce some messages**:
   ```bash
   echo -e "message1\nmessage2\nmessage3" | clik produce test-topic
   ```

5. **Consume them back**:
   ```bash
   clik consume test-topic --from-beginning
   ```

## Output Formats

Most commands support multiple output formats via the `-o, --output` flag:

- **table** (default) - Human-readable tabular output
- **json** - Machine-readable JSON for automation
- **yaml** - YAML format for configuration management
- **name** - Name-only output for scripting
- **Custom format strings** - For produce/consume commands (e.g., `"%k=%v"`)

## Documentation

### Detailed Specifications
- [Context Management](specs/CONTEXT.md) - Managing cluster configurations
- [Topic Management](specs/TOPIC.md) - CRUD operations for topics
- [Consumer Group Management](specs/GROUP.md) - Monitoring and managing consumer groups
- [Cluster Management](specs/CLUSTER.md) - Viewing cluster information and topology
- [ACL Management](specs/ACL.md) - Managing Access Control Lists for security
- [Message Production & Consumption](specs/PRODUCE_CONSUME.md) - Producing and consuming messages

### Development
See [CLAUDE.md](CLAUDE.md) for build instructions, development commands, and architecture details.

## Requirements

- **Runtime**: Java 25+ (JVM mode) or no dependencies (native mode)
- **Kafka**: Tested with 4.1.1
- **Build**: Maven 3.9+ (included wrapper), GraalVM (for native builds)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

[Contributing guidelines to be added]
