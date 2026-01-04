package io.streamshub.clik.config;

import io.streamshub.clik.kafka.KafkaClientType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@ApplicationScoped
public class ContextValidator {

    private static final Pattern CONTEXT_NAME_PATTERN =
            Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$");
    private static final int MAX_CONTEXT_NAME_LENGTH = 253;
    private static final int CONNECTION_TIMEOUT_SECONDS = 5;

    @Inject
    ConfigurationLoader configurationLoader;

    public boolean isValidContextName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }

        if (name.length() > MAX_CONTEXT_NAME_LENGTH) {
            return false;
        }

        return CONTEXT_NAME_PATTERN.matcher(name).matches();
    }

    public ValidationResult validateConfig(ContextConfig config) {
        if (config == null) {
            return ValidationResult.failure("Configuration cannot be null");
        }

        // Check for bootstrap.servers in common properties
        String bootstrapServers = config.getCommon().get("bootstrap.servers");
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            return ValidationResult.failure("Missing required property 'bootstrap.servers'");
        }

        return ValidationResult.success();
    }

    public ValidationResult verifyConnection(ContextConfig config) {
        // Validate config first
        ValidationResult configValidation = validateConfig(config);
        if (!configValidation.valid()) {
            return configValidation;
        }

        // Merge configuration for admin client
        Properties properties = configurationLoader.mergeConfiguration(config, KafkaClientType.ADMIN);

        // Attempt to connect to the cluster
        try (Admin admin = Admin.create(properties)) {
            DescribeClusterOptions options = new DescribeClusterOptions()
                    .timeoutMs((int) Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS).toMillis());

            // Try to describe the cluster to verify connectivity
            admin.describeCluster(options)
                    .clusterId()
                    .get(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            return ValidationResult.success();
        } catch (Exception e) {
            return ValidationResult.failure("Connection failed: " + e.getMessage());
        }
    }
}
