package io.streamshub.clik.config;

import io.quarkus.test.junit.QuarkusTest;
import io.streamshub.clik.kafka.KafkaClientType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class ConfigurationLoaderTest {

    @Inject
    ConfigurationLoader configLoader;

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("clik-test");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    @Test
    void testMergeConfigurationAdmin() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addCommon("security.protocol", "PLAINTEXT")
                .addAdmin("request.timeout.ms", "30000")
                .build();

        Properties props = configLoader.mergeConfiguration(config, KafkaClientType.ADMIN);

        assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
        assertEquals("PLAINTEXT", props.getProperty("security.protocol"));
        assertEquals("30000", props.getProperty("request.timeout.ms"));
    }

    @Test
    void testMergeConfigurationConsumer() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addConsumer("group.id", "test-group")
                .addConsumer("auto.offset.reset", "earliest")
                .build();

        Properties props = configLoader.mergeConfiguration(config, KafkaClientType.CONSUMER);

        assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
        assertEquals("test-group", props.getProperty("group.id"));
        assertEquals("earliest", props.getProperty("auto.offset.reset"));
    }

    @Test
    void testMergeConfigurationProducer() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addProducer("acks", "all")
                .addProducer("compression.type", "snappy")
                .build();

        Properties props = configLoader.mergeConfiguration(config, KafkaClientType.PRODUCER);

        assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
        assertEquals("all", props.getProperty("acks"));
        assertEquals("snappy", props.getProperty("compression.type"));
    }

    @Test
    void testMergeConfigurationOverride() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("client.id", "common-client")
                .addConsumer("client.id", "consumer-client")
                .build();

        Properties props = configLoader.mergeConfiguration(config, KafkaClientType.CONSUMER);

        // Consumer-specific should override common
        assertEquals("consumer-client", props.getProperty("client.id"));
    }

    @Test
    void testParsePropertiesFile() throws IOException {
        Path propsFile = tempDir.resolve("test.properties");
        Files.writeString(propsFile, """
                bootstrap.servers=localhost:9092
                security.protocol=PLAINTEXT
                consumer.group.id=test-group
                producer.acks=all
                admin.request.timeout.ms=30000
                """);

        ContextConfig config = configLoader.parsePropertiesFile(propsFile);

        assertEquals("localhost:9092", config.getCommon().get("bootstrap.servers"));
        assertEquals("PLAINTEXT", config.getCommon().get("security.protocol"));
        assertEquals("test-group", config.getConsumer().get("group.id"));
        assertEquals("all", config.getProducer().get("acks"));
        assertEquals("30000", config.getAdmin().get("request.timeout.ms"));
    }

    @Test
    void testParseYamlFile() throws IOException {
        Path yamlFile = tempDir.resolve("test.yaml");
        Files.writeString(yamlFile, """
                common:
                  bootstrap.servers: localhost:9092
                  security.protocol: PLAINTEXT
                consumer:
                  group.id: test-group
                producer:
                  acks: all
                """);

        ContextConfig config = configLoader.parseYamlFile(yamlFile);

        assertEquals("localhost:9092", config.getCommon().get("bootstrap.servers"));
        assertEquals("PLAINTEXT", config.getCommon().get("security.protocol"));
        assertEquals("test-group", config.getConsumer().get("group.id"));
        assertEquals("all", config.getProducer().get("acks"));
    }

    @Test
    void testToPropertiesFormat() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addCommon("security.protocol", "PLAINTEXT")
                .addAdmin("request.timeout.ms", "30000")
                .addConsumer("group.id", "test-group")
                .addProducer("acks", "all")
                .build();

        String output = configLoader.toPropertiesFormat(config);

        assertTrue(output.contains("# Common configuration"));
        assertTrue(output.contains("bootstrap.servers=localhost:9092"));
        assertTrue(output.contains("security.protocol=PLAINTEXT"));

        assertTrue(output.contains("# Admin configuration"));
        assertTrue(output.contains("admin.request.timeout.ms=30000"));

        assertTrue(output.contains("# Consumer configuration"));
        assertTrue(output.contains("consumer.group.id=test-group"));

        assertTrue(output.contains("# Producer configuration"));
        assertTrue(output.contains("producer.acks=all"));
    }

    @Test
    void testToPropertiesFormatEmptySections() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        String output = configLoader.toPropertiesFormat(config);

        assertTrue(output.contains("# Common configuration"));
        assertTrue(output.contains("bootstrap.servers=localhost:9092"));

        // Empty sections should not appear
        assertFalse(output.contains("# Admin configuration"));
        assertFalse(output.contains("# Consumer configuration"));
        assertFalse(output.contains("# Producer configuration"));
    }
}
