package io.streamshub.clik.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.streamshub.clik.kafka.KafkaClientType;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@ApplicationScoped
public class ConfigurationLoader {

    private static final String CFG_ADMIN = "admin.";
    private static final String CFG_CONSUMER = "consumer.";
    private static final String CFG_PRODUCER = "producer.";

    private final ObjectMapper yamlMapper;

    public ConfigurationLoader() {
        YAMLFactory yamlFactory = YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
        this.yamlMapper = new ObjectMapper(yamlFactory);
    }

    public Properties mergeConfiguration(ContextConfig context, KafkaClientType clientType) {
        Properties properties = new Properties();

        // Start with common properties
        properties.putAll(context.getCommon());

        // Overlay client-type specific properties
        switch (clientType) {
            case ADMIN:
                properties.putAll(context.getAdmin());
                break;
            case CONSUMER:
                properties.putAll(context.getConsumer());
                break;
            case PRODUCER:
                properties.putAll(context.getProducer());
                break;
        }

        return properties;
    }

    public ContextConfig parsePropertiesFile(Path path) {
        Properties props = new Properties();

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            props.load(reader);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load properties file: " + path, e);
        }

        return classifyProperties(props);
    }

    public ContextConfig parseYamlFile(Path path) {
        try {
            ContextConfig config = yamlMapper.readValue(path.toFile(), ContextConfig.class);
            return config != null ? config : new ContextConfig();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load YAML file: " + path, e);
        }
    }

    public String toPropertiesFormat(ContextConfig config) {
        StringWriter writer = new StringWriter();

        // Common section
        if (!config.getCommon().isEmpty()) {
            writer.write("# Common configuration\n");
            writeProperties(writer, config.getCommon(), "");
            writer.write("\n");
        }

        // Admin section
        if (!config.getAdmin().isEmpty()) {
            writer.write("# Admin configuration\n");
            writeProperties(writer, config.getAdmin(), CFG_ADMIN);
            writer.write("\n");
        }

        // Consumer section
        if (!config.getConsumer().isEmpty()) {
            writer.write("# Consumer configuration\n");
            writeProperties(writer, config.getConsumer(), CFG_CONSUMER);
            writer.write("\n");
        }

        // Producer section
        if (!config.getProducer().isEmpty()) {
            writer.write("# Producer configuration\n");
            writeProperties(writer, config.getProducer(), CFG_PRODUCER);
        }

        return writer.toString();
    }

    private void writeProperties(StringWriter writer, Map<String, String> properties, String prefix) {
        properties.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> writer.write(prefix + entry.getKey() + "=" + entry.getValue() + "\n"));
    }

    private ContextConfig classifyProperties(Properties props) {
        Map<String, String> common = new HashMap<>();
        Map<String, String> admin = new HashMap<>();
        Map<String, String> consumer = new HashMap<>();
        Map<String, String> producer = new HashMap<>();

        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);

            if (key.startsWith(CFG_CONSUMER)) {
                consumer.put(key.substring(CFG_CONSUMER.length()), value);
            } else if (key.startsWith(CFG_PRODUCER)) {
                producer.put(key.substring(CFG_PRODUCER.length()), value);
            } else if (key.startsWith(CFG_ADMIN)) {
                admin.put(key.substring(CFG_ADMIN.length()), value);
            } else {
                common.put(key, value);
            }
        }

        return ContextConfig.builder()
                .common(common)
                .admin(admin)
                .consumer(consumer)
                .producer(producer)
                .build();
    }
}
