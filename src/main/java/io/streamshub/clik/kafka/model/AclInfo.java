package io.streamshub.clik.kafka.model;

import java.util.Comparator;

import io.quarkus.runtime.annotations.RegisterForReflection;

import static java.util.Comparator.comparing;

@RegisterForReflection
public record AclInfo(
    String resourceType,      // TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID, DELEGATION_TOKEN, USER
    String resourceName,      // Actual resource name or pattern
    String patternType,       // LITERAL, PREFIXED, MATCH, ANY
    String principal,         // User:alice, User:*, etc.
    String host,              // IP address or *
    String operation,         // READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, etc.
    String permissionType     // ALLOW or DENY
) {

    public static Comparator<AclInfo> comparator() {
        return comparing(AclInfo::resourceType)
                .thenComparing(AclInfo::resourceName)
                .thenComparing(AclInfo::principal)
                .thenComparing(AclInfo::operation);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String resourceType;
        private String resourceName;
        private String patternType;
        private String principal;
        private String host;
        private String operation;
        private String permissionType;

        public Builder resourceType(String resourceType) {
            this.resourceType = resourceType;
            return this;
        }

        public Builder resourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder patternType(String patternType) {
            this.patternType = patternType;
            return this;
        }

        public Builder principal(String principal) {
            this.principal = principal;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder operation(String operation) {
            this.operation = operation;
            return this;
        }

        public Builder permissionType(String permissionType) {
            this.permissionType = permissionType;
            return this;
        }

        public AclInfo build() {
            return new AclInfo(
                resourceType,
                resourceName,
                patternType,
                principal,
                host,
                operation,
                permissionType
            );
        }
    }
}
