package io.streamshub.clik.kafka;

import java.util.List;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class KafkaClientFactoryTest extends ClikTestBase {

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

    @Override
    @AfterEach
    protected void tearDown() {
        // Clean up all contexts after each test
        List<String> contexts = contextService.listContexts();
        for (String context : contexts) {
            try {
                contextService.deleteContext(context);
            } catch (Exception _) {
                // Ignore
            }
        }

        super.tearDown();
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
