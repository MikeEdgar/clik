package io.streamshub.clik.kafka;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(KafkaClientFactoryTest.TestConfig.class)
class KafkaClientFactoryTest {

    public static class TestConfig implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            try {
                Path tempDir = Files.createTempDirectory("clik-test");
                return Map.of("xdg.config.home", tempDir.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    ContextService contextService;

    @BeforeEach
    void setUp() {
        // Create a test context
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test-context", config, false);
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up all contexts after each test
        List<String> contexts = contextService.listContexts();
        for (String context : contexts) {
            try {
                contextService.deleteContext(context);
            } catch (Exception e) {
                // Ignore
            }
        }

        // Clean up config directory
        Path configDir = contextService.getConfigDirectory();
        if (Files.exists(configDir)) {
            Files.walk(configDir)
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
    void testCreateAdminClientFromCurrentContext() {
        contextService.setCurrentContext("test-context");

        Admin admin = clientFactory.createAdminClient();
        assertNotNull(admin);
        admin.close();
    }

    @Test
    void testCreateAdminClientNoCurrentContext() {
        // Don't set current context
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                clientFactory.createAdminClient());

        assertTrue(exception.getMessage().contains("No current context set"));
    }

    @Test
    void testCreateAdminClientFromSpecificContext() {
        Admin admin = clientFactory.createAdminClient("test-context");
        assertNotNull(admin);
        admin.close();
    }

    @Test
    void testCreateAdminClientFromNonExistentContext() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                clientFactory.createAdminClient("nonexistent-context"));

        assertTrue(exception.getMessage().contains("does not exist"));
    }
}
