package io.streamshub.clik.config;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ContextValidator that require a running Kafka instance.
 * Uses Kafka Dev Services which automatically starts a Kafka broker for tests.
 */
@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class ContextValidatorKafkaTest extends ClikTestBase {

    @Inject
    ContextValidator validator;

    @Test
    void testVerifyConnectionSuccess() {
        // Create a context config with the dev services Kafka bootstrap servers
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", kafkaBootstrapServers())
                .build();

        // Verify connection should succeed with dev services Kafka
        ValidationResult result = validator.verifyConnection(config);

        assertTrue(result.isValid(), "Connection verification should succeed with dev services Kafka");
    }

    @Test
    void testVerifyConnectionFailure() {
        // Create a context config with invalid bootstrap servers
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "invalid-host:9092")
                .build();

        // Verify connection should fail
        ValidationResult result = validator.verifyConnection(config);

        assertFalse(result.isValid(), "Connection verification should fail with invalid servers");
        assertTrue(result.getMessage().contains("Connection failed"),
                "Error message should indicate connection failure");
    }

    @Test
    void testVerifyConnectionMissingBootstrapServers() {
        // Create a context config without bootstrap servers
        ContextConfig config = ContextConfig.builder().build();

        // Verify connection should fail
        ValidationResult result = validator.verifyConnection(config);

        assertFalse(result.isValid(), "Connection verification should fail without bootstrap servers");
        assertTrue(result.getMessage().contains("bootstrap.servers"),
                "Error message should mention missing bootstrap.servers");
    }
}
