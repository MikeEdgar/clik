package io.streamshub.clik.kafka.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FeatureInfo(
    String name,                    // e.g., "metadata.version"
    Short finalizedMinVersion,      // null if not finalized
    Short finalizedMaxVersion,      // null if not finalized
    Short supportedMinVersion,      // null if not supported
    Short supportedMaxVersion,      // null if not supported
    String status,                  // "FINALIZED", "SUPPORTED_ONLY", "UNKNOWN"
    String kafkaVersion            // e.g., "4.2" (only for metadata.version)
) {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private Short finalizedMinVersion;
        private Short finalizedMaxVersion;
        private Short supportedMinVersion;
        private Short supportedMaxVersion;
        private String status;
        private String kafkaVersion;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder finalizedMinVersion(Short finalizedMinVersion) {
            this.finalizedMinVersion = finalizedMinVersion;
            return this;
        }

        public Builder finalizedMaxVersion(Short finalizedMaxVersion) {
            this.finalizedMaxVersion = finalizedMaxVersion;
            return this;
        }

        public Builder supportedMinVersion(Short supportedMinVersion) {
            this.supportedMinVersion = supportedMinVersion;
            return this;
        }

        public Builder supportedMaxVersion(Short supportedMaxVersion) {
            this.supportedMaxVersion = supportedMaxVersion;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public Builder kafkaVersion(String kafkaVersion) {
            this.kafkaVersion = kafkaVersion;
            return this;
        }

        public FeatureInfo build() {
            return new FeatureInfo(
                name,
                finalizedMinVersion,
                finalizedMaxVersion,
                supportedMinVersion,
                supportedMaxVersion,
                status,
                kafkaVersion
            );
        }
    }
}
