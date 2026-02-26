# Kafka Feature Management Specification

## Overview

This specification defines feature management commands for Clik, enabling operators to view and modify Kafka cluster features. Features represent capabilities that can be enabled, upgraded, or downgraded across the cluster, with the most critical being `metadata.version` which controls the Kafka version level.

## Goals

- Provide visibility into all cluster features (both finalized and supported)
- Enable safe feature upgrades during cluster migrations
- Support controlled feature downgrades with appropriate safeguards
- Map metadata.version to Kafka release versions for clarity
- Integrate seamlessly with context management for cluster configuration
- Follow Kafka AdminClient patterns for feature manipulation

## Non-Goals

- Feature creation (features are cluster-defined)
- Automatic feature rollback on failure
- Feature dependency validation (delegated to Kafka broker)
- Rolling feature upgrades (cluster-wide operation)

## User Stories

1. **As an operator**, I want to view all cluster features to understand the current capability set before planning an upgrade
2. **As a platform engineer**, I want to upgrade metadata.version to enable new Kafka features after a broker upgrade
3. **As an SRE**, I want to see supported version ranges to understand safe upgrade paths
4. **As a cluster administrator**, I want to downgrade a feature if it causes compatibility issues
5. **As a DevOps engineer**, I want to map metadata.version to Kafka release versions to verify cluster state

## Command Structure

All feature management commands are under the `clik feature` subcommand group.

### Command: `clik feature list`

List all cluster features with their version ranges.

**Syntax:**
```bash
clik feature list [OPTIONS]
```

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json | table |

**Examples:**

```bash
# List all features (table format)
clik feature list

# List features as YAML
clik feature list -o yaml

# List features as JSON
clik feature list -o json
```

**Output (table format):**
```
FEATURE              STATUS          FINALIZED  SUPPORTED  KAFKA VERSION
metadata.version     FINALIZED       28-29      1-29       4.2
transaction.version  FINALIZED       1          1-1        -
test.feature         SUPPORTED_ONLY  -          1-5        -
```

**Output (JSON format):**
```json
[
  {
    "name": "metadata.version",
    "finalizedMinVersion": 28,
    "finalizedMaxVersion": 29,
    "supportedMinVersion": 1,
    "supportedMaxVersion": 29,
    "status": "FINALIZED",
    "kafkaVersion": "4.2"
  },
  {
    "name": "transaction.version",
    "finalizedMinVersion": 1,
    "finalizedMaxVersion": 1,
    "supportedMinVersion": 1,
    "supportedMaxVersion": 1,
    "status": "FINALIZED",
    "kafkaVersion": null
  }
]
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. Call `admin.describeFeatures()` to get feature metadata
4. Merge finalized and supported feature maps
5. Map `metadata.version` max level to Kafka version string
6. Sort features by name
7. Format output according to `--output` flag

**Feature Status:**
- `FINALIZED` - Feature has been finalized and is active in the cluster
- `SUPPORTED_ONLY` - Feature is supported but not finalized

**Version Range Display:**
- Single version: `5` (when min == max)
- Version range: `1-29` (when min != max)
- No version: `-` (when not finalized or not supported)

**Kafka Version Mapping:**
Only `metadata.version` feature includes a Kafka version mapping. The mapping uses:
- `MetadataVersion.fromFeatureLevel(level).shortVersion()` for known versions
- `"Unknown (<X.Y)"` for versions below minimum
- `"Unknown (>X.Y)"` for versions above maximum
- `"Unknown"` if feature not finalized

### Command: `clik feature describe`

Display detailed information about a specific feature.

**Syntax:**
```bash
clik feature describe <name> [OPTIONS]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `name` | Feature name (e.g., metadata.version) |

**Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <format>` | Output format: table, yaml, json | table |

**Examples:**

```bash
# Describe metadata.version feature
clik feature describe metadata.version

# Output as YAML
clik feature describe metadata.version -o yaml

# Output as JSON
clik feature describe transaction.version -o json
```

**Output (table format):**
```
Feature: metadata.version
Status: FINALIZED

Finalized Version Range:
  Min: 28
  Max: 29

Supported Version Range:
  Min: 1
  Max: 29

Kafka Version: 4.2
```

**Output (JSON format):**
```json
{
  "name": "metadata.version",
  "finalizedMinVersion": 28,
  "finalizedMaxVersion": 29,
  "supportedMinVersion": 1,
  "supportedMaxVersion": 29,
  "status": "FINALIZED",
  "kafkaVersion": "4.2"
}
```

**Behavior:**

1. Load configuration from current context
2. Create AdminClient with context configuration
3. Call `admin.describeFeatures()` to get feature metadata
4. Look up feature by name in both finalized and supported maps
5. Return error if feature not found
6. Map `metadata.version` to Kafka version if applicable
7. Format output according to `--output` flag

**Error Cases:**
- Feature not found: Exit code 1 with message "Error: Feature \"<name>\" not found."

### Command: `clik feature alter`

Alter feature level (upgrade, downgrade, or disable).

**Syntax:**
```bash
clik feature alter <name> [--upgrade <level> | --downgrade <level> | --disable] [OPTIONS]
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `name` | Feature name (e.g., metadata.version) |

**Alter Options (mutually exclusive, one required):**

| Flag | Description |
|------|-------------|
| `--upgrade <level>` | Upgrade to specified version level |
| `--downgrade <level>` | Downgrade to specified version level (requires confirmation) |
| `--disable` | Disable/reset feature to level 0 (requires confirmation) |

**Additional Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-y, --yes` | Skip confirmation prompt | false |

**Examples:**

```bash
# Upgrade metadata.version to level 30
clik feature alter metadata.version --upgrade 30

# Downgrade with confirmation prompt
clik feature alter metadata.version --downgrade 28

# Downgrade without confirmation prompt
clik feature alter metadata.version --downgrade 28 -y

# Disable a feature (with confirmation)
clik feature alter test.feature --disable

# Disable a feature (auto-confirm)
clik feature alter test.feature --disable --yes
```

**Behavior - Upgrade:**

1. Verify feature exists via `describeFeature()`
2. Validate target level > current finalized max
3. Validate target level <= supported max
4. Call `admin.updateFeatures()` with `allowDowngrade = false`
5. Display success message

**Behavior - Downgrade:**

1. Verify feature exists and is finalized
2. Validate target level < current finalized max
3. Validate target level >= supported min
4. Display warning: "WARNING: Downgrading features can cause cluster instability."
5. Prompt for confirmation (unless `-y` flag)
6. If confirmed, call `admin.updateFeatures()` with `allowDowngrade = true`
7. Display success message

**Behavior - Disable:**

1. Verify feature exists and is finalized
2. Display warning: "WARNING: Disabling features can cause cluster instability."
3. Prompt for confirmation (unless `-y` flag)
4. If confirmed, call `admin.updateFeatures()` with target level 0 and `allowDowngrade = true`
5. Display success message

**Validation Rules:**

Upgrade validation:
- Feature must exist
- Target level must be greater than current finalized max
- Target level must not exceed supported max

Downgrade validation:
- Feature must exist and be finalized
- Target level must be less than current finalized max
- Target level must not be below supported min

Disable validation:
- Feature must exist and be finalized

**Confirmation Prompts:**

Downgrade prompt:
```
WARNING: Downgrading features can cause cluster instability.
Downgrade feature "metadata.version" from 29 to 28? [y/N]:
```

Disable prompt:
```
WARNING: Disabling features can cause cluster instability.
Disable feature "test.feature"? This cannot be undone. [y/N]:
```

**Error Cases:**

- Feature not found: Exit code 1 with message "Error: Feature \"<name>\" not found."
- Invalid upgrade level (too high): Exit code 1 with message "Error: Target level X exceeds maximum supported level Y"
- Invalid upgrade level (not higher): Exit code 1 with message "Error: Target level X must be greater than current finalized level Y"
- Invalid downgrade level (too low): Exit code 1 with message "Error: Target level X is below minimum supported level Y"
- Invalid downgrade level (not lower): Exit code 1 with message "Error: Target level X must be less than current finalized level Y"
- Feature not finalized (downgrade): Exit code 1 with message "Error: Feature \"<name>\" is not finalized and cannot be downgraded."
- Feature not finalized (disable): Exit code 1 with message "Error: Feature \"<name>\" is not finalized and cannot be disabled."
- User cancels confirmation: Exit code 0 with message "Downgrade cancelled." or "Disable cancelled."
- Kafka API error: Exit code 1 with message "Error: Failed to alter feature: <error message>"

## Implementation Details

### Data Model

**FeatureInfo Record:**
```java
@RegisterForReflection
public record FeatureInfo(
    String name,                    // e.g., "metadata.version"
    Short finalizedMinVersion,      // null if not finalized
    Short finalizedMaxVersion,      // null if not finalized
    Short supportedMinVersion,      // null if not supported
    Short supportedMaxVersion,      // null if not supported
    String status,                  // "FINALIZED", "SUPPORTED_ONLY", "UNKNOWN"
    String kafkaVersion            // e.g., "4.2" (only for metadata.version)
)
```

### Service Layer

**FeatureService Methods:**
- `List<FeatureInfo> listFeatures(Admin admin)` - List all features
- `FeatureInfo describeFeature(Admin admin, String featureName)` - Describe specific feature
- `void updateFeature(Admin admin, String featureName, short targetVersion, boolean allowDowngrade)` - Update feature
- `void disableFeature(Admin admin, String featureName)` - Disable feature (reset to 0)

### Admin API Usage

**Describe Features:**
```java
var describeResult = admin.describeFeatures();
var metadata = describeResult.featureMetadata()
    .toCompletionStage()
    .toCompletableFuture()
    .join();

Map<String, FinalizedVersionRange> finalizedFeatures = metadata.finalizedFeatures();
Map<String, SupportedVersionRange> supportedFeatures = metadata.supportedFeatures();
```

**Update Feature:**
```java
Map<String, FeatureUpdate> updates = Map.of(
    featureName,
    new FeatureUpdate(targetVersion, allowDowngrade
        ? FeatureUpdate.UpgradeType.SAFE_DOWNGRADE
        : FeatureUpdate.UpgradeType.UPGRADE)
);

admin.updateFeatures(updates, null)
    .all()
    .toCompletionStage()
    .toCompletableFuture()
    .join();
```

### Metadata Version Mapping

Reuses logic from `ClusterService.determineFeatureLevel()`:
```java
private String mapMetadataVersionToKafkaVersion(short versionLevel) {
    try {
        return MetadataVersion.fromFeatureLevel(versionLevel).shortVersion();
    } catch (IllegalArgumentException _) {
        if (versionLevel < MetadataVersion.MINIMUM_VERSION.featureLevel()) {
            return "Unknown (<%s)".formatted(MetadataVersion.MINIMUM_VERSION.shortVersion());
        }
        return "Unknown (>%s)".formatted(MetadataVersion.latestTesting().shortVersion());
    }
}
```

## Kafka Version Compatibility

- **Minimum Kafka version**: 2.8.0 (KIP-584: Versioning scheme for features)
- **Recommended**: Kafka 3.3+ (improved feature stability)
- **Tested with**: Kafka 4.1.1

**Older Kafka versions:**
- Kafka < 2.8: Feature management not supported (API does not exist)
- Kafka 2.8 - 3.2: Basic feature support, limited metadata.version levels

## Security Considerations

**Required ACLs:**
- `DESCRIBE` on `CLUSTER` resource - for `list` and `describe` commands
- `ALTER` on `CLUSTER` resource - for `alter` command

**Authorization errors:**
```
Error: Failed to list features: org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.
```

## Testing Strategy

**Unit Tests (FeatureServiceTest):**
- List features returns non-empty collection
- Describe feature found vs not found
- Kafka version mapping for metadata.version
- Status field correctly set (FINALIZED vs SUPPORTED_ONLY)
- Builder pattern works correctly

**Integration Tests (FeatureCommandTest):**
- List features in all output formats (table, yaml, json)
- Describe feature in all output formats
- Describe feature not found returns error
- Alter with invalid options (missing, invalid upgrade, invalid downgrade)
- Alter feature not found returns error

**Manual Testing:**
- Actual feature upgrade (requires cluster with upgrade path)
- Actual feature downgrade (requires test cluster)
- Confirmation prompts display and flush correctly
- Feature disable operation
- Error messages are clear and actionable

## Future Enhancements

1. **Feature validation** - Pre-flight checks before upgrade/downgrade
2. **Feature dependencies** - Display which features depend on others
3. **Upgrade planning** - Suggest safe upgrade paths
4. **Rollback support** - Automatic rollback on feature update failure
5. **Batch operations** - Update multiple features atomically
6. **Feature history** - Track feature changes over time
7. **Safe mode** - Additional validation and dry-run support

## Related Kafka Improvement Proposals

- KIP-584: Versioning scheme for features
- KIP-778: KRaft metadata transactions
- KIP-853: KRaft Controller Membership Changes

## References

- [Kafka AdminClient Documentation](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/Admin.html)
- [KIP-584: Versioning scheme for features](https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features)
- [MetadataVersion Javadoc](https://kafka.apache.org/41/javadoc/org/apache/kafka/server/common/MetadataVersion.html)
