# Kafka Context Management Specification

## Overview

This specification defines a context management system for Clik, enabling users to save and switch between multiple Kafka cluster configurations. Contexts are stored in XDG-compliant configuration directories and support different configurations for admin, consumer, and producer client types.

## Goals

- Provide a kubectl-like context experience for Kafka cluster management
- Support multiple environments (dev, staging, prod) with easy switching
- Organize configuration by Kafka client type (admin, consumer, producer)
- Follow XDG Base Directory specification for proper config organization
- Enable both file-based and CLI-based context creation

## Non-Goals (Future Enhancements)

- Credential encryption/credential helpers (v2)
- Context templates or inheritance (v2)
- Auto-discovery of Kafka clusters (v2)
- Context export/import (v2)
- Namespace/project organization (v2)

## User Stories

1. **As a developer**, I want to save my local Kafka cluster config so I don't have to type connection details repeatedly
2. **As an operator**, I want to switch between dev/staging/prod clusters quickly and safely
3. **As a data engineer**, I want different consumer and producer configs per environment
4. **As a team member**, I want to share context files (without credentials) via documentation

## Directory Structure

Following XDG Base Directory Specification, contexts are stored in `$XDG_CONFIG_HOME/clik` (defaults to `~/.config/clik`):

```
~/.config/clik/
├── config.yaml                  # Root configuration (current context, global settings)
├── contexts/
│   ├── dev/
│   │   └── config.yaml         # Dev context configuration
│   ├── staging/
│   │   └── config.yaml         # Staging context configuration
│   └── prod/
│       └── config.yaml         # Production context configuration
```

### Root Configuration Format

**File**: `~/.config/clik/config.yaml`

```yaml
# Current active context
current-context: dev

# Global CLI settings (future)
# output-format: table
# log-level: info
```

### Context Configuration Format

**File**: `~/.config/clik/contexts/<context-name>/config.yaml`

```yaml
# Common configuration applied to all Kafka client types
common:
  bootstrap.servers: localhost:9092
  security.protocol: PLAINTEXT
  client.id: clik-${client-type}

# Admin client specific configuration
admin:
  request.timeout.ms: 30000

# Consumer client specific configuration
consumer:
  group.id: clik-consumer-group
  auto.offset.reset: earliest
  enable.auto.commit: true
  max.poll.records: 500

# Producer client specific configuration
producer:
  acks: all
  compression.type: snappy
  max.in.flight.requests.per.connection: 5
  enable.idempotence: true
```

**Configuration Merging Rules:**

1. Start with `common` properties
2. Overlay client-type specific section (`admin`, `consumer`, or `producer`)
3. Client-type properties override common properties with the same key

**Example merged configuration for consumer:**
```properties
bootstrap.servers=localhost:9092
security.protocol=PLAINTEXT
client.id=clik-consumer
group.id=clik-consumer-group
auto.offset.reset=earliest
enable.auto.commit=true
max.poll.records=500
```

## Command Structure

All context management commands are under the `clik context` subcommand group.

### Command: `clik context create`

Create a new context with Kafka configuration.

**Syntax:**
```bash
clik context create <name> [OPTIONS]
```

**Options:**

| Flag | Description | Required | Example |
|------|-------------|----------|---------|
| `--bootstrap-servers <servers>` | Comma-separated Kafka broker addresses | Yes | `localhost:9092` |
| `--from-file <path>` | Load configuration from existing properties/YAML file | No | `kafka.properties` |
| `--security-protocol <protocol>` | Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) | No | `SASL_SSL` |
| `--sasl-mechanism <mechanism>` | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, etc.) | No | `SCRAM-SHA-512` |
| `--property <key=value>` | Additional Kafka property (repeatable) | No | `--property acks=all` |
| `--property-file <path>` | Load additional properties from file | No | `consumer.properties` |
| `--verify` | Verify connection to Kafka cluster after creation | No | - |
| `--overwrite` | Overwrite existing context if it exists | No | - |

**Examples:**

```bash
# Create simple local development context
clik context create dev --bootstrap-servers localhost:9092

# Create context from existing properties file
clik context create staging --from-file staging-kafka.properties

# Create production context with SASL authentication
clik context create prod \
  --bootstrap-servers prod1.kafka:9092,prod2.kafka:9092,prod3.kafka:9092 \
  --security-protocol SASL_SSL \
  --sasl-mechanism SCRAM-SHA-512 \
  --property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="secret";'

# Create and verify connection
clik context create qa \
  --bootstrap-servers qa.kafka:9092 \
  --verify

# Create with consumer-specific settings
clik context create analytics \
  --bootstrap-servers localhost:9092 \
  --property consumer.group.id=analytics-group \
  --property consumer.auto.offset.reset=earliest

# Create with producer-specific settings
clik context create streaming \
  --bootstrap-servers localhost:9092 \
  --property producer.acks=all \
  --property producer.compression.type=zstd
```

**Behavior:**

1. Validate context name (alphanumeric, hyphens, underscores only)
2. Check if context already exists (error unless `--overwrite`)
3. Create directory: `$XDG_CONFIG_HOME/clik/contexts/<name>/`
4. Parse configuration from flags and/or files
5. Organize properties into `common`, `admin`, `consumer`, `producer` sections
6. Write `config.yaml` to context directory
7. If `--verify`, attempt connection to cluster using admin client
8. Print success message with context name

**Property Section Detection:**

Properties are assigned to sections based on prefix:
- `consumer.*` → `consumer` section
- `producer.*` → `producer` section
- `admin.*` → `admin` section
- Everything else → `common` section

When using `--from-file` with a flat properties file, all properties go to `common` section unless they have a recognized prefix.

**Error Conditions:**

- Invalid context name format
- Context already exists (without `--overwrite`)
- Missing required `--bootstrap-servers` (unless `--from-file` provides it)
- Invalid file path for `--from-file` or `--property-file`
- Connection verification fails with `--verify`
- Invalid YAML/properties format in input files

### Command: `clik context list`

List all available contexts.

**Syntax:**
```bash
clik context list [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json, name | table |
| `--show-current` | Highlight current active context | true |

**Examples:**

```bash
# List all contexts (table format)
clik context list

# List contexts as names only
clik context list -o name

# List contexts with full configuration
clik context list -o yaml
```

**Output (table format):**
```
CURRENT   NAME       BOOTSTRAP SERVERS              SECURITY
*         dev        localhost:9092                 PLAINTEXT
          staging    staging.kafka:9092             SASL_SSL
          prod       prod1.kafka:9092,prod2...      SASL_SSL
```

**Output (name format):**
```
dev
staging
prod
```

**Behavior:**

1. Scan `$XDG_CONFIG_HOME/clik/contexts/` directory
2. Read each context's `config.yaml`
3. Load current context from root `config.yaml`
4. Format output according to `--output` flag
5. Mark current context with `*` in table format

### Command: `clik context use`

Switch to a different context (set as current).

**Syntax:**
```bash
clik context use <name>
```

**Examples:**

```bash
# Switch to production context
clik context use prod

# Switch to development context
clik context use dev
```

**Behavior:**

1. Validate that context exists
2. Update `current-context` in root `config.yaml`
3. Print confirmation message: `Switched to context "prod"`

**Error Conditions:**

- Context does not exist

### Command: `clik context current`

Display the current active context.

**Syntax:**
```bash
clik context current [OPTIONS]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--describe` | Display full configuration of current context |

**Examples:**

```bash
# Show current context name
clik context current

# Show current context with full configuration
clik context current --describe
```

**Output (name only):**
```
dev
```

**Output (with --describe):**
```
Current context: dev

Configuration:
  common:
    bootstrap.servers: localhost:9092
    security.protocol: PLAINTEXT
  consumer:
    group.id: clik-consumer-group
    auto.offset.reset: earliest
  producer:
    acks: all
    compression.type: snappy
```

**Behavior:**

1. Read `current-context` from root `config.yaml`
2. If `--describe`, load and display context configuration
3. Print context name and optionally configuration

**Error Conditions:**

- No current context set
- Current context no longer exists (deleted)

### Command: `clik context delete`

Delete a context.

**Syntax:**
```bash
clik context delete <name> [OPTIONS]
```

**Options:**

| Flag | Description |
|------|-------------|
| `-y, --yes` | Automatically confirm deletion without prompting |

**Examples:**

```bash
# Delete context with confirmation
clik context delete old-cluster

# Delete context without confirmation
clik context delete old-cluster --yes
```

**Behavior:**

1. Validate context exists
2. Check if context is currently active
3. Prompt for confirmation (unless `--yes`)
4. Delete context directory: `$XDG_CONFIG_HOME/clik/contexts/<name>/`
5. If deleting current context, clear `current-context` in root config
6. Print confirmation message

**Error Conditions:**

- Context does not exist
- User declines confirmation prompt

### Command: `clik context describe`

Display detailed configuration for a specific context.

**Syntax:**
```bash
clik context describe <name> [OPTIONS]
```

**Options:**

| Flag | Description |
|------|-------------|
| `-o, --output <format>` | Output format: yaml, json, properties | yaml |

**Examples:**

```bash
# Show context configuration (YAML)
clik context describe prod

# Show as Java properties format
clik context describe prod -o properties

# Show as JSON
clik context describe prod -o json
```

**Output (YAML):**
```yaml
common:
  bootstrap.servers: prod.kafka:9092
  security.protocol: SASL_SSL
admin:
  request.timeout.ms: 30000
consumer:
  group.id: prod-consumer
producer:
  acks: all
```

**Output (properties):**
```properties
# Common configuration
bootstrap.servers=prod.kafka:9092
security.protocol=SASL_SSL

# Admin configuration
admin.request.timeout.ms=30000

# Consumer configuration
consumer.group.id=prod-consumer

# Producer configuration
producer.acks=all
```

**Behavior:**

1. Validate context exists
2. Load context configuration from `config.yaml`
3. Format according to `--output` flag
4. Print configuration

### Command: `clik context rename`

Rename an existing context.

**Syntax:**
```bash
clik context rename <old-name> <new-name>
```

**Examples:**

```bash
# Rename a context
clik context rename dev development

# Rename the current context
clik context rename staging stage
```

**Behavior:**

1. Validate old context exists
2. Validate new context name format (alphanumeric, hyphens, underscores)
3. Check that new context name doesn't already exist
4. Rename the context directory from `contexts/<old-name>` to `contexts/<new-name>`
5. If the old context was current, update `current-context` in root config
6. Print success message

**Error Conditions:**

- Old context does not exist
- New context name is invalid format
- New context name already exists

## Integration with Kafka Commands

All Kafka operation commands (topics, consumer groups, etc.) will accept a `--context` flag.

**Global Option (added to future commands):**

```bash
--context <name>     Use specific context for this command
```

**Examples:**

```bash
# List topics using prod context
clik topics list --context prod

# Describe consumer group using staging context
clik consumer-groups describe mygroup --context staging

# Create topic using current context (default)
clik topics create my-topic --partitions 3
```

**Configuration Resolution:**

1. If `--context` provided, load that context's configuration
2. Otherwise, load current context from root config
3. If no current context, require explicit `--bootstrap-servers` flag
4. Merge context config based on command's client type:
   - Topic commands → use `common` + `admin`
   - Consumer commands → use `common` + `consumer`
   - Producer commands → use `common` + `producer`

## Architecture & Implementation

### Package Structure

```
io.streamshub.clik/
├── command/
│   ├── context/
│   │   ├── ContextCommand.java           # Parent @Command
│   │   ├── CreateContextCommand.java
│   │   ├── ListContextsCommand.java
│   │   ├── UseContextCommand.java
│   │   ├── CurrentContextCommand.java
│   │   ├── DeleteContextCommand.java
│   │   ├── ShowContextCommand.java
│   │   └── RenameContextCommand.java
├── config/
│   ├── ContextConfig.java                # Context YAML structure
│   ├── RootConfig.java                   # Root config.yaml structure
│   ├── ContextService.java               # Context CRUD operations
│   ├── ConfigurationLoader.java          # Load and merge configs
│   └── ContextValidator.java             # Validation logic
├── kafka/
│   └── KafkaClientFactory.java           # Create clients from config
└── Clik.java                             # Updated with context subcommand
```

### Core Services

#### ContextService

```java
@ApplicationScoped
public class ContextService {

    /**
     * Get the XDG config directory, defaulting to ~/.config/clik
     */
    Path getConfigDirectory();

    /**
     * Create a new context
     */
    void createContext(String name, ContextConfig config, boolean overwrite);

    /**
     * List all available contexts
     */
    List<String> listContexts();

    /**
     * Get a context by name
     */
    Optional<ContextConfig> getContext(String name);

    /**
     * Delete a context
     */
    void deleteContext(String name);

    /**
     * Rename a context
     */
    void renameContext(String oldName, String newName);

    /**
     * Check if context exists
     */
    boolean contextExists(String name);

    /**
     * Get current context name from root config
     */
    Optional<String> getCurrentContext();

    /**
     * Set current context in root config
     */
    void setCurrentContext(String name);

    /**
     * Load context configuration
     */
    ContextConfig loadContext(String name);

    /**
     * Save context configuration
     */
    void saveContext(String name, ContextConfig config);
}
```

#### ConfigurationLoader

```java
@ApplicationScoped
public class ConfigurationLoader {

    /**
     * Merge context configuration into Kafka properties
     * for a specific client type
     */
    Properties mergeConfiguration(
        ContextConfig context,
        KafkaClientType clientType
    );

    /**
     * Parse properties file into ContextConfig structure
     */
    ContextConfig parsePropertiesFile(Path path);

    /**
     * Parse YAML file into ContextConfig structure
     */
    ContextConfig parseYamlFile(Path path);

    /**
     * Convert ContextConfig to properties format
     */
    String toPropertiesFormat(ContextConfig config);
}
```

#### ContextValidator

```java
@ApplicationScoped
public class ContextValidator {

    /**
     * Validate context name format
     */
    boolean isValidContextName(String name);

    /**
     * Validate context configuration
     */
    ValidationResult validateConfig(ContextConfig config);

    /**
     * Test connection to Kafka cluster
     */
    ValidationResult verifyConnection(ContextConfig config);
}
```

### Data Structures

#### ContextConfig

```java
public class ContextConfig {
    private Map<String, String> common = new HashMap<>();
    private Map<String, String> admin = new HashMap<>();
    private Map<String, String> consumer = new HashMap<>();
    private Map<String, String> producer = new HashMap<>();

    // Getters, setters, builder
}
```

#### RootConfig

```java
public class RootConfig {
    private String currentContext;
    private Map<String, String> settings = new HashMap<>();

    // Getters, setters
}
```

#### KafkaClientType

```java
public enum KafkaClientType {
    ADMIN,
    CONSUMER,
    PRODUCER
}
```

### Command Implementation Example

```java
@CommandLine.Command(
    name = "create",
    description = "Create a new Kafka context"
)
public class CreateContextCommand implements Callable<Integer> {

    @CommandLine.Parameters(
        index = "0",
        description = "Context name"
    )
    String name;

    @CommandLine.Option(
        names = {"--bootstrap-servers"},
        description = "Kafka broker addresses",
        required = false
    )
    String bootstrapServers;

    @CommandLine.Option(
        names = {"--from-file"},
        description = "Load configuration from file"
    )
    Path fromFile;

    @CommandLine.Option(
        names = {"--property"},
        description = "Kafka property (repeatable)"
    )
    Map<String, String> properties;

    @CommandLine.Option(
        names = {"--verify"},
        description = "Verify connection"
    )
    boolean verify;

    @Inject
    ContextService contextService;

    @Inject
    ContextValidator validator;

    @Inject
    ConfigurationLoader configLoader;

    @Override
    public Integer call() {
        // Validate name
        if (!validator.isValidContextName(name)) {
            System.err.println("Invalid context name: " + name);
            return 1;
        }

        // Check existence
        if (contextService.contextExists(name) && !overwrite) {
            System.err.println("Context already exists: " + name);
            return 1;
        }

        // Build configuration
        ContextConfig config = buildConfig();

        // Validate
        ValidationResult result = validator.validateConfig(config);
        if (!result.isValid()) {
            System.err.println("Invalid configuration: " + result.getMessage());
            return 1;
        }

        // Verify connection if requested
        if (verify) {
            ValidationResult connResult = validator.verifyConnection(config);
            if (!connResult.isValid()) {
                System.err.println("Connection failed: " + connResult.getMessage());
                return 1;
            }
        }

        // Save
        contextService.createContext(name, config, overwrite);

        System.out.println("Context \"" + name + "\" created.");
        return 0;
    }

    private ContextConfig buildConfig() {
        // Implementation details...
    }
}
```

## Testing Strategy

### Unit Tests

1. **ContextService Tests**
   - Create context with valid config
   - Create context with invalid name
   - Overwrite existing context
   - List contexts (empty, single, multiple)
   - Delete context
   - Get/set current context

2. **ConfigurationLoader Tests**
   - Merge common + admin configs
   - Merge common + consumer configs
   - Merge common + producer configs
   - Client-specific override of common properties
   - Parse properties file
   - Parse YAML file
   - Convert to properties format

3. **ContextValidator Tests**
   - Valid context names (alphanumeric, hyphens, underscores)
   - Invalid context names (special chars, spaces, empty)
   - Valid configurations (required fields present)
   - Invalid configurations (missing bootstrap.servers)

### Integration Tests

1. **End-to-End Context Flow**
   - Create context → list → use → current → delete
   - Create from properties file
   - Create from YAML file
   - Create with verification (requires test Kafka cluster)

2. **Kafka Command Integration**
   - Use context with topics command
   - Use context with consumer groups command
   - Verify correct client configuration applied

### Test Data

Example test contexts in `src/test/resources/contexts/`:

```
contexts/
├── simple-local.yaml
├── sasl-ssl-prod.yaml
├── invalid-missing-servers.yaml
└── legacy.properties
```

## Migration & Compatibility

### Initial Setup

On first run, if `$XDG_CONFIG_HOME/clik/` doesn't exist:
1. Create directory structure
2. Create empty root `config.yaml`
3. Optionally create default "local" context

### Backward Compatibility

Since this is a new feature:
- No migration from previous versions needed
- All existing commands continue to work
- `--bootstrap-servers` flag remains supported for ad-hoc usage

### Future Migration Path

If context format changes in future versions:
1. Add `version` field to context config
2. Implement migration logic in ContextService
3. Auto-migrate on first load

## Security Considerations

### File Permissions

- Context directories should be created with `700` permissions
- Context config files should be created with `600` permissions
- Root config should be `600` permissions
- Document in README: never commit context files with credentials

### Credential Management

**Current (v1):**
- Plain text storage in YAML files
- User responsible for file permissions and .gitignore

**Future (v2):**
- Credential helper plugins (similar to docker/kubectl)
- Environment variable substitution: `${KAFKA_PASSWORD}`
- Integration with system keychains (macOS Keychain, GNOME Keyring)

### Sensitive Data in Logs

- Never log full configuration (may contain credentials)
- Redact sensitive properties in error messages:
  - `sasl.jaas.config`
  - `ssl.keystore.password`
  - `ssl.truststore.password`
  - Any property containing "password", "secret", "key"

### Connection Verification

The `--verify` flag performs these checks:
1. Create AdminClient with context configuration
2. Call `describeCluster()` with 5-second timeout
3. Verify broker connectivity
4. Close client

Verification does NOT:
- Validate authentication credentials fully (only connection)
- Test consumer/producer configs (only admin client)
- Perform authorization checks

## Error Messages & User Experience

### Error Message Guidelines

1. **Clear and actionable**
   ```
   Error: Context "prod" does not exist.

   Run 'clik context list' to see available contexts.
   ```

2. **Provide context**
   ```
   Error: Invalid context name "my context" (contains spaces).

   Context names must contain only letters, numbers, hyphens, and underscores.
   ```

3. **Suggest fixes**
   ```
   Error: Missing required property 'bootstrap.servers'.

   Provide servers with: --bootstrap-servers localhost:9092
   Or load from file with: --from-file kafka.properties
   ```

### Success Messages

```
Context "dev" created.
Switched to context "prod".
Context "old-cluster" deleted.
```

### Interactive Prompts

```bash
$ clik context delete prod
Delete context "prod"? This cannot be undone. [y/N]:
```

## Configuration Examples

### Example 1: Local Development

```yaml
# ~/.config/clik/contexts/dev/config.yaml
common:
  bootstrap.servers: localhost:9092
  security.protocol: PLAINTEXT

consumer:
  group.id: dev-consumer
  auto.offset.reset: earliest

producer:
  acks: 1
```

### Example 2: Production with SASL/SSL

```yaml
# ~/.config/clik/contexts/prod/config.yaml
common:
  bootstrap.servers: prod1.kafka:9092,prod2.kafka:9092,prod3.kafka:9092
  security.protocol: SASL_SSL
  sasl.mechanism: SCRAM-SHA-512
  sasl.jaas.config: |
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="admin"
    password="secret123";
  ssl.truststore.location: /etc/kafka/truststore.jks
  ssl.truststore.password: truststorepass

admin:
  request.timeout.ms: 60000

consumer:
  group.id: prod-consumer
  auto.offset.reset: latest
  enable.auto.commit: false
  isolation.level: read_committed

producer:
  acks: all
  compression.type: zstd
  max.in.flight.requests.per.connection: 5
  enable.idempotence: true
  transactional.id: prod-transactions
```

### Example 3: Consumer-Optimized Context

```yaml
# ~/.config/clik/contexts/analytics/config.yaml
common:
  bootstrap.servers: analytics.kafka:9092

consumer:
  group.id: analytics-consumer
  auto.offset.reset: earliest
  max.poll.records: 1000
  fetch.min.bytes: 1048576
  fetch.max.wait.ms: 500
  session.timeout.ms: 30000
  max.partition.fetch.bytes: 10485760
```

## Implementation Phases

### Phase 1: Core Context Management (MVP) ✅ COMPLETED
- [x] ContextService implementation
- [x] ConfigurationLoader implementation
- [x] ContextValidator implementation
- [x] Root config management
- [x] `clik context create` command
- [x] `clik context list` command
- [x] `clik context use` command
- [x] `clik context current` command
- [x] `clik context delete` command
- [x] `clik context describe` command
- [x] Unit tests for all services
- [x] `--from-file` support for multiple formats (properties, YAML)
- [x] `--verify` connection testing
- [x] Better error messages and validation
- [x] Multiple output formats (table, yaml, json, name, properties)
- [x] XDG Base Directory specification compliance
- [x] Jackson-based YAML/JSON serialization
- [x] MicroProfile Config integration for XDG path
- [x] `--property` flag for inline properties
- [x] `--property-file` for additional property files
- [x] File permission management (700 for dirs, 600 for files)

### Phase 2: Enhanced Context Features
- [x] Integration tests for context commands
- [x] Context rename command
- [ ] Shell completion for context names
- [ ] Context update command

### Phase 3: Advanced Features (Future)
- [ ] Credential helpers/plugins
- [ ] Environment variable substitution
- [ ] Context templates
- [ ] Context export/import
- [ ] Context namespaces/projects

## References

- [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir/latest/)
- [Apache Kafka Configuration](https://kafka.apache.org/documentation/#configuration)
- [Picocli User Manual](https://picocli.info/)
- [Kubectl Config Documentation](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_config/)

## Appendices

### Appendix A: Context Name Validation Regex

```java
private static final Pattern CONTEXT_NAME_PATTERN =
    Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$");
```

Rules:
- Must start with letter or number
- May contain letters, numbers, hyphens, underscores
- Minimum length: 1 character
- Maximum length: 253 characters (following DNS subdomain rules)

### Appendix B: Property Classification

**Common Properties:**
- `bootstrap.servers`
- `client.id`
- `security.protocol`
- `sasl.mechanism`
- `sasl.jaas.config`
- `ssl.*`
- `request.timeout.ms`
- `retries`

**Admin-Specific Properties:**
- `default.api.timeout.ms`
- Properties with `admin.` prefix

**Consumer-Specific Properties:**
- `group.id`
- `auto.offset.reset`
- `enable.auto.commit`
- `max.poll.records`
- `fetch.min.bytes`
- Properties with `consumer.` prefix

**Producer-Specific Properties:**
- `acks`
- `compression.type`
- `enable.idempotence`
- `transactional.id`
- `max.in.flight.requests.per.connection`
- Properties with `producer.` prefix

### Appendix C: XDG Environment Variables

```bash
# Default locations if not set
XDG_CONFIG_HOME=${HOME}/.config
XDG_DATA_HOME=${HOME}/.local/share
XDG_CACHE_HOME=${HOME}/.cache
XDG_STATE_HOME=${HOME}/.local/state

# Clik uses
XDG_CONFIG_HOME=/path/to/configs  # Context configs
XDG_CACHE_HOME=/path/to/cache     # Future: cache cluster metadata
```

### Appendix D: Sample .gitignore

```gitignore
# Clik context files (may contain credentials)
.config/clik/contexts/*/config.yaml

# Allow sharing template without credentials
!.config/clik/contexts/template/config.yaml
```
