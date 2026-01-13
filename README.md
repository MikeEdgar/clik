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

Single-session

```shell
source <(clik generate-completion)
```

Persist in user preferences

```shell
clik generate-completion > ~/.local/share/bash-completion/clik
```

Persist globally

```shell
# Linux
clik generate-completion > /etc/bash_completion.d/clik

# MacOS
clik generate-completion > /usr/local/etc/bash_completion.d/clik
```

## Features

### 🔧 [Context Management](specs/CONTEXT.md)
Save and switch between multiple Kafka cluster configurations with kubectl-like context commands.

```bash
# Create a context for your local cluster
clik context create local --bootstrap-servers localhost:9092

# Switch to production
clik context use production

# View current context
clik context current
```

### 📊 [Topic Management](specs/TOPIC.md)
Create, list, describe, alter, and delete Kafka topics with rich output formatting.

```bash
# Create a topic with 3 partitions
clik topic create my-topic --partitions 3 --replication-factor 1

# List all topics
clik topic list

# Describe a topic with partition details
clik topic describe my-topic

# View as JSON for automation
clik topic list -o json
```

### 👥 [Consumer Group Management](specs/GROUP.md)
Monitor consumer lag, manage offsets, and control consumer group lifecycle. Supports all Kafka 4.1+ group types (consumer, classic, share, streams).

```bash
# List all consumer groups
clik group list

# Describe a group with lag information
clik group describe my-consumer-group

# Reset offsets to earliest
clik group alter my-group --to-earliest --yes

# Shift back 1 hour to reprocess messages
clik group alter my-group --by-duration PT-1H --yes

# Delete an inactive group
clik group delete old-group --yes
```

### 🖥️ [Cluster Management](specs/CLUSTER.md)
View detailed cluster information including nodes, controllers, and KRaft quorum metadata. Supports both KRaft and ZooKeeper deployments.

```bash
# View cluster details with node roles
clik cluster describe
# Shows: Cluster ID, Feature Level, Controller ID, Nodes
# Displays: Node roles (Broker, Controller, Broker+Controller)
# Displays: Quorum roles (Voter, Observer) and leader

# Export cluster topology as JSON
clik cluster describe -o json
```

### 🔒 [ACL Management](specs/ACL.md)
Manage Kafka Access Control Lists (ACLs) to secure your cluster resources. Grant or deny access to topics, consumer groups, and cluster operations.

```bash
# Grant read access to a user on a topic
clik acl create --topic my-topic --operation READ --principal User:alice

# Grant write access with a prefixed pattern
clik acl create --topic orders --pattern-type PREFIXED --operation WRITE --principal User:producer

# List all ACLs
clik acl list

# List ACLs for a specific resource
clik acl list --topic my-topic

# Delete ACL with confirmation
clik acl delete --topic my-topic --principal User:alice --operation READ

# Delete all ACLs for a principal
clik acl delete --principal User:olduser --yes
```

### 📨 [Message Production](specs/PRODUCE_CONSUME.md)
Produce messages from files, stdin, or interactively with support for keys, headers, and timestamps.

```bash
# Produce from a file
clik produce my-topic --file messages.txt

# Pipe data to a topic
echo "Hello Kafka" | clik produce my-topic

# Produce with a key and header
clik produce my-topic --value "important data" \
  --key user123 \
  --header "content-type=application/json"

# Structured input with format strings
cat data.csv | clik produce my-topic --input "%k,%v"
```

### 📥 [Message Consumption](specs/PRODUCE_CONSUME.md)
Consume messages with flexible offset control, output formatting, and continuous monitoring.

```bash
# Consume from beginning
clik consume my-topic --from-beginning

# Continuous consumption (like tail -f)
clik consume my-topic --follow

# Consume specific partition from offset
clik consume my-topic --partition 2 --from-offset 1000

# Custom output format
clik consume my-topic --from-beginning -o "%k: %v"

# Export to JSON for processing
clik consume my-topic --from-beginning -o json > messages.json
```

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

## Example Workflows

### Development Testing
```bash
# Set up dev environment
clik context create dev --bootstrap-servers dev-kafka:9092
clik context use dev

# Create topic for testing
clik topic create user-events --partitions 3 --replication-factor 1

# Produce test data
clik produce user-events --file test-data.json

# Verify messages
clik consume user-events --from-beginning --max-messages 10
```

### Debugging Consumer Lag
```bash
# Check which groups are behind
clik group list

# See detailed lag information
clik group describe slow-consumer-group

# Reset to reprocess last hour of data
clik group alter slow-consumer-group --by-duration PT-1H --yes
```

### Monitoring Production
```bash
# Switch to production context
clik context use production

# Verify cluster topology
clik cluster describe

# Monitor new messages in real-time
clik consume critical-events --follow --from-end

# Check consumer group health
clik group describe payment-processor
```

### Cluster Inspection
```bash
# View cluster information
clik cluster describe
```

### Data Migration
```bash
# Export messages from source
clik context use source-cluster
clik consume legacy-topic --from-beginning -o "%v" > data.txt

# Import to destination
clik context use dest-cluster
clik produce new-topic --file data.txt
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
