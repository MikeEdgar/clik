# Kafka ACL Management Specification

## Overview

This specification defines Access Control List (ACL) management commands for Clik, enabling users to create, list, and delete Kafka ACLs. The design follows established patterns from topic and group management while addressing Kafka-specific requirements for authorization and security.

## Goals

- Provide intuitive CLI commands for common ACL operations
- Support all Kafka resource types (topics, groups, cluster, transactional IDs, etc.)
- Support all pattern types (literal, prefixed, match, any)
- Enable flexible filtering for listing and deleting ACLs
- Integrate seamlessly with context management for cluster configuration
- Use resource-specific shortcuts for better user experience

## Non-Goals (Future Enhancements)

- ACL templates or presets (v2)
- Bulk ACL operations from file (v2)
- ACL migration between clusters (v2)
- Role-based access control (RBAC) abstraction (v2)

## User Stories

1. **As a security admin**, I want to grant read access to specific users on topics for secure data access
2. **As a platform engineer**, I want to list all ACLs for a topic to audit permissions
3. **As a team lead**, I want to revoke access for users who have left the team
4. **As a developer**, I want to grant my application access to consumer groups for testing
5. **As an operations engineer**, I want to allow specific hosts to access cluster resources

## Command Structure

All ACL management commands are under the `clik acl` subcommand group.

### Command: `clik acl create`

Create a new ACL binding.

**Syntax:**
```bash
clik acl create --principal <principal> --operation <operation> [RESOURCE] [OPTIONS]
```

**Required Options:**

| Flag | Description |
|------|-------------|
| `--principal, -p <principal>` | Principal in format: User:\<username\> or User:* for all users |
| `--operation <operation>` | Operation to allow/deny (READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, etc.) |

**Resource Options (one required):**

| Flag | Description |
|------|-------------|
| `--topic <name>` | Topic resource |
| `--group <name>` | Consumer group resource |
| `--cluster` | Cluster resource (name defaults to "kafka-cluster") |
| `--transactional-id <id>` | Transactional ID resource |
| `--delegation-token <id>` | Delegation token resource |
| `--user-resource <name>` | User resource |

**Additional Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `--permission <type>` | Permission type: ALLOW or DENY | ALLOW |
| `--host <host>` | Host from which principal can access | * (all hosts) |
| `--pattern-type <type>` | Pattern type: LITERAL or PREFIXED | LITERAL |

**Examples:**

```bash
# Grant read access to alice on a specific topic
clik acl create --topic my-topic --operation READ --principal User:alice

# Grant write access with prefixed pattern
clik acl create --topic orders --pattern-type PREFIXED --operation WRITE --principal User:producer

# Deny write access to all users
clik acl create --topic sensitive --operation WRITE --principal User:* --permission DENY

# Grant access from specific host
clik acl create --topic my-topic --operation READ --principal User:alice --host 192.168.1.100

# Grant cluster-level access
clik acl create --cluster --operation ALTER --principal User:admin

# Grant consumer group access
clik acl create --group my-group --operation READ --principal User:consumer
```

**Behavior:**

1. Load configuration from current context
2. Validate principal format (must start with "User:")
3. Validate that exactly one resource option is specified
4. Create AdminClient with context configuration
5. Build AclBinding with specified parameters
6. Create ACL using Kafka Admin API
7. Print success message with ACL details

**Output:**
```
ACL created successfully.
  Principal: User:alice
  Resource: TOPIC:my-topic (LITERAL)
  Operation: READ
  Permission: ALLOW
  Host: *
```

**Error Conditions:**

- Invalid principal format (must be "User:\<username\>" or "User:*")
- No resource specified
- Multiple resources specified
- Invalid operation value
- Invalid permission type
- Invalid pattern type
- No current context set
- Authorization failure

### Command: `clik acl list`

List ACL bindings with optional filtering.

**Syntax:**
```bash
clik acl list [OPTIONS]
```

**Filter Options (all optional):**

| Flag | Description |
|------|-------------|
| `--principal <principal>` | Filter by principal |
| `--host <host>` | Filter by host |
| `--operation <operation>` | Filter by operation |
| `--permission <type>` | Filter by permission type |
| `--topic <name>` | Filter by topic resource |
| `--group <name>` | Filter by consumer group resource |
| `--cluster` | Filter by cluster resource |
| `--transactional-id <id>` | Filter by transactional ID resource |
| `--delegation-token <id>` | Filter by delegation token resource |
| `--user-resource <name>` | Filter by user resource |
| `--pattern-type <type>` | Filter by pattern type |

**Output Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json | table |

**Examples:**

```bash
# List all ACLs
clik acl list

# List ACLs for a specific topic
clik acl list --topic my-topic

# List ACLs for a specific principal
clik acl list --principal User:alice

# List ACLs with multiple filters
clik acl list --topic my-topic --principal User:alice --operation READ

# List in JSON format
clik acl list -o json

# List in YAML format
clik acl list -o yaml
```

**Output (table format):**
```
PRINCIPAL      RESOURCE TYPE  RESOURCE NAME  PATTERN   OPERATION  PERMISSION  HOST
User:alice     TOPIC          my-topic       LITERAL   READ       ALLOW       *
User:bob       TOPIC          orders         PREFIXED  WRITE      ALLOW       192.168.1.100
User:admin     CLUSTER        kafka-cluster  LITERAL   ALTER      ALLOW       *
```

**Output (JSON format):**
```json
[
  {
    "resourceType": "TOPIC",
    "resourceName": "my-topic",
    "patternType": "LITERAL",
    "principal": "User:alice",
    "host": "*",
    "operation": "READ",
    "permissionType": "ALLOW"
  },
  {
    "resourceType": "TOPIC",
    "resourceName": "orders",
    "patternType": "PREFIXED",
    "principal": "User:bob",
    "host": "192.168.1.100",
    "operation": "WRITE",
    "permissionType": "ALLOW"
  }
]
```

**Output (YAML format):**
```yaml
- resourceType: TOPIC
  resourceName: my-topic
  patternType: LITERAL
  principal: User:alice
  host: "*"
  operation: READ
  permissionType: ALLOW
- resourceType: TOPIC
  resourceName: orders
  patternType: PREFIXED
  principal: User:bob
  host: 192.168.1.100
  operation: WRITE
  permissionType: ALLOW
```

**Behavior:**

1. Load configuration from current context
2. Build AclBindingFilter from provided filter options (null = ANY)
3. Create AdminClient with context configuration
4. Query ACLs using Kafka Admin API
5. Convert results to AclInfo domain model
6. Sort by resourceType, resourceName, principal, operation
7. Format output according to `--output` flag

**Note:** When no filters are specified, lists all ACLs (uses AclBindingFilter.ANY).

### Command: `clik acl delete`

Delete ACL bindings matching filter criteria.

**Syntax:**
```bash
clik acl delete [FILTERS] [OPTIONS]
```

**Filter Options (at least one required):**

| Flag | Description |
|------|-------------|
| `--principal <principal>` | Filter by principal |
| `--host <host>` | Filter by host |
| `--operation <operation>` | Filter by operation |
| `--permission <type>` | Filter by permission type |
| `--topic <name>` | Filter by topic resource |
| `--group <name>` | Filter by consumer group resource |
| `--cluster` | Filter by cluster resource |
| `--transactional-id <id>` | Filter by transactional ID resource |
| `--delegation-token <id>` | Filter by delegation token resource |
| `--user-resource <name>` | Filter by user resource |
| `--pattern-type <type>` | Filter by pattern type |

**Options:**

| Flag | Description |
|------|-------------|
| `-y, --yes` | Automatically confirm deletion without prompting |

**Examples:**

```bash
# Delete specific ACL (with confirmation)
clik acl delete --topic my-topic --principal User:alice --operation READ

# Delete with auto-confirm
clik acl delete --principal User:alice --yes

# Delete all ACLs for a topic
clik acl delete --topic old-topic --yes

# Delete by operation
clik acl delete --operation WRITE --topic my-topic --yes
```

**Output (with confirmation):**
```
The following ACL bindings will be deleted:
  User:alice on TOPIC:my-topic (READ, ALLOW)

Delete 1 ACL binding? This cannot be undone. [y/N]: y
1 ACL binding deleted.
```

**Output (without confirmation):**
```
2 ACL bindings deleted.
```

**Behavior:**

1. Validate that at least one filter is specified
2. Load configuration from current context
3. Build AclBindingFilter from provided filter options
4. Query matching ACLs to show preview
5. If no matches found, exit with error
6. If `--yes` not specified, show preview and prompt for confirmation
7. Build specific filters for each matched ACL (to ensure only previewed ACLs are deleted)
8. Delete ACLs using Kafka Admin API
9. Print count of deleted ACLs

**Error Conditions:**

- No filters specified
- No ACLs matched the specified criteria
- User declined confirmation
- No current context set
- Authorization failure

**Safety Feature:** The delete operation first queries matching ACLs, shows them to the user (unless `--yes`), then builds specific filters for each matched ACL. This ensures only the ACLs that were previewed and confirmed are deleted, preventing accidental deletion of ACLs that might match a broad filter.

## Integration with Context Management

All ACL commands use the current context for Kafka cluster configuration.

**Context Resolution:**

1. Load current context from root config
2. If no current context, fail with error
3. Merge context config using `common` + `admin` sections
4. Create AdminClient with merged configuration

**Example with contexts:**

```bash
# Set production context
clik context use production

# Create ACL in production cluster (uses current context)
clik acl create --topic critical-events --operation READ --principal User:monitor

# List ACLs in production
clik acl list

# Switch to dev context
clik context use dev

# List ACLs in dev cluster
clik acl list
```

## Architecture & Implementation

### Package Structure

```
io.streamshub.clik/
├── command/
│   ├── acl/
│   │   ├── AclCommand.java              # Parent @Command
│   │   ├── CreateAclCommand.java        # Create ACLs
│   │   ├── ListAclsCommand.java         # List ACLs
│   │   ├── DeleteAclCommand.java        # Delete ACLs
│   │   └── options/
│   │       ├── Resource.java            # Resource option mixins
│   │       ├── Operation.java           # Operation option mixins
│   │       ├── Permission.java          # Permission option mixins
│   │       └── PatternType.java         # Pattern type option mixins
├── kafka/
│   ├── KafkaClientFactory.java          # Create AdminClient from context
│   ├── AclService.java                  # ACL CRUD operations
│   └── model/
│       └── AclInfo.java                 # ACL metadata
└── Clik.java                            # Updated with ACL subcommand
```

### Core Services

#### AclService

```java
@ApplicationScoped
public class AclService {

    /**
     * Create ACL bindings
     */
    public void createAcls(
        Admin admin,
        Collection<AclBinding> aclBindings
    ) throws ExecutionException, InterruptedException;

    /**
     * List ACLs matching filter
     */
    public List<AclInfo> listAcls(
        Admin admin,
        AclBindingFilter filter
    ) throws ExecutionException, InterruptedException;

    /**
     * Delete ACLs matching filters
     */
    public Collection<AclInfo> deleteAcls(
        Admin admin,
        Collection<AclBindingFilter> filters
    ) throws ExecutionException, InterruptedException;

    /**
     * Build AclBinding for create operations
     */
    public AclBinding buildAclBinding(
        String resourceType,
        String resourceName,
        String patternType,
        String principal,
        String host,
        String operation,
        String permissionType
    );

    /**
     * Build AclBindingFilter for list/delete operations
     * (null parameters = ANY)
     */
    public AclBindingFilter buildAclBindingFilter(
        String resourceType,
        String resourceName,
        String patternType,
        String principal,
        String host,
        String operation,
        String permissionType
    );
}
```

### Data Models

#### AclInfo

```java
@RegisterForReflection
public record AclInfo(
    String resourceType,      // TOPIC, GROUP, CLUSTER, etc.
    String resourceName,      // Resource name or pattern
    String patternType,       // LITERAL, PREFIXED, MATCH
    String principal,         // User:alice, User:*, etc.
    String host,              // IP or *
    String operation,         // READ, WRITE, CREATE, etc.
    String permissionType     // ALLOW or DENY
) {
    public static Builder builder() { /* ... */ }

    public static class Builder {
        // Fluent builder for AclInfo construction
    }
}
```

### Option Mixins

To avoid code duplication, resource, operation, permission, and pattern type options are defined as reusable mixin classes using Picocli's `@Mixin` annotation.

#### Resource Options

```java
public record Resource(String type, String name) {
    public static final Resource UNSPECIFIED = new Resource(null, null);

    public static class Options {
        @CommandLine.Option(names = {"--topic"})
        String topic;

        @CommandLine.Option(names = {"--group"})
        String group;

        @CommandLine.Option(names = {"--cluster"}, arity="0..1", fallbackValue = "kafka-cluster")
        String cluster;

        @CommandLine.Option(names = {"--transactional-id"})
        String transactionalId;

        @CommandLine.Option(names = {"--delegation-token"})
        String delegationToken;

        @CommandLine.Option(names = {"--user-resource"})
        String user;

        public Resource resource() {
            // Return Resource based on which field is set
        }
    }

    public static Resource fromOptions(Options options) {
        return options == null ? UNSPECIFIED : options.resource();
    }

    public boolean isMissing() {
        return type == null;
    }
}
```

Used with `@CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")` for list/delete or `multiplicity = "1"` for create to ensure mutual exclusivity.

#### Operation Options

Provides `ValueOption` (required, for create) and `FilterOption` (optional, for list/delete):

```java
public class Operation {
    public static class ValueOption {
        @CommandLine.Option(
            names = {"--operation"},
            required = true,
            description = "Operation: READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, etc."
        )
        String operation;

        public String value() { return operation; }
    }

    public static class FilterOption {
        @CommandLine.Option(
            names = {"--operation"},
            description = "Filter by operation"
        )
        String operation;

        public String value() { return operation; }
    }
}
```

#### Permission Options

Similar to Operation, provides `ValueOption` and `FilterOption` for permission types.

#### Pattern Type Options

Similar to Operation, provides `ValueOption` (defaults to LITERAL) and `FilterOption` for pattern types.

## Testing Strategy

### Unit Tests

**AclCommandTest.java** - Comprehensive test coverage with 31 tests:

1. **Create ACL Tests (10 tests)**
   - Create ACL for topic, group, cluster
   - Create with prefixed pattern type
   - Create with DENY permission
   - Create with custom host
   - Create for transactional ID
   - Validation errors (invalid principal, invalid operation)
   - No context error

2. **List ACLs Tests (9 tests)**
   - List when empty
   - List in table format
   - List in JSON format
   - List in YAML format
   - Filter by topic
   - Filter by principal
   - Filter by operation
   - Multiple filters
   - No context error

3. **Delete ACL Tests (7 tests)**
   - Delete single ACL
   - Delete multiple ACLs
   - Delete with auto-confirm (`--yes`)
   - Delete with no matches (error)
   - Delete without filters (error)
   - Delete by principal only
   - No context error

4. **Edge Case Tests (5 tests)**
   - Wildcard principal (User:*)
   - All resource types
   - Invalid output format
   - Invalid permission type

### Integration Tests

**AclCommandIT.java** - Native image integration test wrapper:

```java
@QuarkusMainIntegrationTest
@TestProfile(ClikMainTestBase.Profile.class)
@Tag("integration-test")
class AclCommandIT extends AclCommandTest {
    // Inherits all 31 tests, runs against native executable
}
```

### Test Infrastructure

- Uses Quarkus Dev Services with Kafka container
- Kafka configured with StandardAuthorizer for ACL support
- Super user configured as `User:ANONYMOUS` to allow test operations
- Each test creates fresh context and cleans up after execution

**application.properties configuration:**
```properties
quarkus.kafka.devservices.image-name=quay.io/medgar/kafka-native:999-SNAPSHOT
quarkus.kafka.devservices.container-env."KAFKA_AUTHORIZER_CLASS_NAME"=org.apache.kafka.metadata.authorizer.StandardAuthorizer
quarkus.kafka.devservices.container-env."KAFKA_SUPER_USERS"=User:ANONYMOUS
```

## Error Messages & User Experience

### Error Message Guidelines

1. **Clear and actionable**
   ```
   Error: Invalid principal format. Must be 'User:<username>' or 'User:*'
   ```

2. **Provide context**
   ```
   Error: At least one filter must be specified for delete operation.

   Available filters:
     --principal <user>       Filter by principal
     --topic <name>           Filter by topic
     --group <name>           Filter by consumer group
     --cluster                Filter by cluster
     --operation <op>         Filter by operation
     --permission <type>      Filter by permission type
     --host <host>            Filter by host
     --pattern-type <type>    Filter by pattern type
   ```

3. **Suggest fixes**
   ```
   Error: No current context set.

   Set a context with: clik context use <name>
   Or create one with: clik context create <name> --bootstrap-servers <servers>
   ```

### Success Messages

```
ACL created successfully.
1 ACL binding deleted.
2 ACL bindings deleted.
No ACLs found.
```

### Interactive Prompts

```bash
$ clik acl delete --topic my-topic --principal User:alice --operation READ
The following ACL bindings will be deleted:
  User:alice on TOPIC:my-topic (READ, ALLOW)

Delete 1 ACL binding? This cannot be undone. [y/N]:
```

## Security Considerations

### Authorization Requirements

ACL operations require appropriate Kafka cluster permissions:

- **Create ACL**: Requires `ALTER` permission on `CLUSTER` resource
- **List ACL**: Requires `DESCRIBE` permission on `CLUSTER` resource
- **Delete ACL**: Requires `ALTER` permission on `CLUSTER` resource

### Best Practices

1. **Principle of Least Privilege**: Grant only necessary permissions
2. **Use Specific Resources**: Prefer specific resource names over wildcards
3. **Review Before Delete**: Always review ACL preview before confirming deletion
4. **Audit ACL Changes**: Regularly list and review ACLs
5. **Use DENY Sparingly**: DENY rules take precedence; use with caution

### Common Patterns

**Read-Only Consumer:**
```bash
clik acl create --topic events --operation READ --principal User:consumer
clik acl create --group events-consumer --operation READ --principal User:consumer
```

**Producer Application:**
```bash
clik acl create --topic events --operation WRITE --principal User:producer
clik acl create --topic events --operation DESCRIBE --principal User:producer
```

**Admin User:**
```bash
clik acl create --cluster --operation ALTER --principal User:admin
clik acl create --cluster --operation DESCRIBE --principal User:admin
```

## Implementation Status

### Phase 1: Core ACL Operations (Completed)
- [x] AclService implementation with create, list, delete operations
- [x] AclInfo domain model with @RegisterForReflection
- [x] `clik acl create` command with resource shortcuts
- [x] `clik acl list` command with filtering and output formats
- [x] `clik acl delete` command with confirmation prompts
- [x] Resource, Operation, Permission, PatternType option mixins
- [x] PrintWriter pattern for output (out()/err() methods)
- [x] Unit tests (31 tests in AclCommandTest)
- [x] Integration test wrapper (AclCommandIT)
- [x] Registered in Clik.java
- [x] Current context integration

### Phase 2: Enhancements (Future)
- [ ] ACL templates or presets for common scenarios
- [ ] Bulk ACL operations from YAML/JSON file
- [ ] ACL migration between clusters
- [ ] Better error messages for authorization failures
- [ ] ACL diff/compare between environments

## References

- [Apache Kafka Authorization](https://kafka.apache.org/documentation/#security_authz)
- [Kafka AdminClient API - ACL Operations](https://kafka.apache.org/documentation/#adminapi)
- [Kafka ACL Documentation](https://kafka.apache.org/documentation/#security_authz_primitives)
- [Picocli User Manual](https://picocli.info/)

## Appendices

### Appendix A: ACL Operations

**Valid Operations:**
- `READ` - Read from a resource
- `WRITE` - Write to a resource
- `CREATE` - Create a resource
- `DELETE` - Delete a resource
- `ALTER` - Alter a resource
- `DESCRIBE` - Describe a resource
- `CLUSTER_ACTION` - Cluster-level action
- `DESCRIBE_CONFIGS` - Describe configurations
- `ALTER_CONFIGS` - Alter configurations
- `IDEMPOTENT_WRITE` - Idempotent write
- `CREATE_TOKENS` - Create delegation tokens
- `DESCRIBE_TOKENS` - Describe delegation tokens
- `ALL` - All operations

### Appendix B: Resource Types

**Supported Resource Types:**
- `TOPIC` - Kafka topic
- `GROUP` - Consumer group
- `CLUSTER` - Kafka cluster
- `TRANSACTIONAL_ID` - Transactional ID
- `DELEGATION_TOKEN` - Delegation token
- `USER` - User resource

### Appendix C: Pattern Types

**Pattern Types:**
- `LITERAL` - Exact match (default)
- `PREFIXED` - Prefix match (e.g., "orders-" matches "orders-2024", "orders-prod")
- `MATCH` - Any resource (wildcard)
- `ANY` - Any pattern type (for filters only)

### Appendix D: Principal Format

Kafka ACL principals must follow the format: `<PrincipalType>:<PrincipalName>`

**Examples:**
- `User:alice` - Specific user
- `User:*` - All users
- `User:CN=alice,OU=Engineering,O=Company` - User with Distinguished Name (for TLS auth)

**Note:** Clik currently supports only `User:` principal type, which is the most common. Other principal types (like `Group:`) may be added in future versions based on Kafka cluster authentication configuration.
