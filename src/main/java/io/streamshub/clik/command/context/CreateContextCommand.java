package io.streamshub.clik.command.context;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.config.ConfigurationLoader;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.config.ContextValidator;
import io.streamshub.clik.config.ValidationResult;
import picocli.CommandLine;

@CommandLine.Command(
        name = "create",
        description = "Create a new Kafka context"
)
public class CreateContextCommand extends BaseCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Context name"
    )
    String name;

    @CommandLine.Option(
            names = {"--bootstrap-servers"},
            description = "Comma-separated Kafka broker addresses"
    )
    String bootstrapServers;

    @CommandLine.Option(
            names = {"--from-file"},
            description = "Load configuration from existing properties/YAML file"
    )
    Path fromFile;

    @CommandLine.Option(
            names = {"--security-protocol"},
            description = "Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)"
    )
    String securityProtocol;

    @CommandLine.Option(
            names = {"--sasl-mechanism"},
            description = "SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, etc.)"
    )
    String saslMechanism;

    @CommandLine.Option(
            names = {"--property", "-P"},
            description = "Additional Kafka property (repeatable, format: key=value)"
    )
    Map<String, String> properties = new HashMap<>();

    @CommandLine.Option(
            names = {"--property-file"},
            description = "Load additional properties from file"
    )
    Path propertyFile;

    @CommandLine.Option(
            names = {"--verify"},
            description = "Verify connection to Kafka cluster after creation"
    )
    boolean verify;

    @CommandLine.Option(
            names = {"--overwrite"},
            description = "Overwrite existing context if it exists"
    )
    boolean overwrite;

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
            err().println("Error: Invalid context name \"" + name + "\".");
            err().println();
            err().println("Context names must contain only letters, numbers, hyphens, and underscores.");
            return 1;
        }

        // Check existence
        if (contextService.contextExists(name) && !overwrite) {
            err().println("Error: Context \"" + name + "\" already exists.");
            err().println();
            err().println("Use --overwrite to replace it.");
            return 1;
        }

        // Build configuration
        ContextConfig config;
        try {
            config = buildConfig();
        } catch (Exception e) {
            err().println("Error: Failed to build configuration: " + e.getMessage());
            return 1;
        }

        // Validate
        ValidationResult result = validator.validateConfig(config);
        if (!result.valid()) {
            err().println("Error: " + result.message());
            err().println();
            err().println("Provide servers with: --bootstrap-servers localhost:9092");
            err().println("Or load from file with: --from-file kafka.properties");
            return 1;
        }

        // Save context
        try {
            contextService.createContext(name, config, overwrite);
        } catch (Exception e) {
            err().println("Error: Failed to create context: " + e.getMessage());
            return 1;
        }

        // Verify connection if requested
        if (verify) {
            out().println("Verifying connection to Kafka cluster...");
            ValidationResult connResult = validator.verifyConnection(config);
            if (!connResult.valid()) {
                err().println("Error: " + connResult.message());
                err().println();
                err().println("Context was created but connection verification failed.");
                return 1;
            }
            out().println("Connection verified successfully.");
        }

        out().println("Context \"" + name + "\" created.");
        return 0;
    }

    private ContextConfig buildConfig() {
        ContextConfig.Builder builder = ContextConfig.builder();

        // Load from file if specified
        if (fromFile != null) {
            ContextConfig fileConfig;
            String fileName = fromFile.getFileName().toString().toLowerCase();

            if (fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
                fileConfig = configLoader.parseYamlFile(fromFile);
            } else {
                fileConfig = configLoader.parsePropertiesFile(fromFile);
            }

            // Start with file config
            builder.common(new HashMap<>(fileConfig.getCommon()))
                    .admin(new HashMap<>(fileConfig.getAdmin()))
                    .consumer(new HashMap<>(fileConfig.getConsumer()))
                    .producer(new HashMap<>(fileConfig.getProducer()));
        }

        // Load from property file if specified
        if (propertyFile != null) {
            ContextConfig propFileConfig = configLoader.parsePropertiesFile(propertyFile);
            mergeConfig(builder, propFileConfig);
        }

        // Add bootstrap.servers if provided
        if (bootstrapServers != null) {
            builder.addCommon("bootstrap.servers", bootstrapServers);
        }

        // Add security.protocol if provided
        if (securityProtocol != null) {
            builder.addCommon("security.protocol", securityProtocol);
        }

        // Add sasl.mechanism if provided
        if (saslMechanism != null) {
            builder.addCommon("sasl.mechanism", saslMechanism);
        }

        // Add additional properties
        if (properties != null && !properties.isEmpty()) {
            classifyAndAddProperties(builder, properties);
        }

        return builder.build();
    }

    private void mergeConfig(ContextConfig.Builder builder, ContextConfig source) {
        source.getCommon().forEach(builder::addCommon);
        source.getAdmin().forEach(builder::addAdmin);
        source.getConsumer().forEach(builder::addConsumer);
        source.getProducer().forEach(builder::addProducer);
    }

    private void classifyAndAddProperties(ContextConfig.Builder builder, Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith("consumer.")) {
                builder.addConsumer(key.substring("consumer.".length()), value);
            } else if (key.startsWith("producer.")) {
                builder.addProducer(key.substring("producer.".length()), value);
            } else if (key.startsWith("admin.")) {
                builder.addAdmin(key.substring("admin.".length()), value);
            } else {
                builder.addCommon(key, value);
            }
        }
    }
}
