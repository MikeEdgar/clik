package io.streamshub.clik.config;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.test.ClikTestBase;

import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class ContextValidatorTest extends ClikTestBase {

    @Inject
    ContextValidator validator;

    @Test
    void testValidContextNames() {
        assertTrue(validator.isValidContextName("dev"));
        assertTrue(validator.isValidContextName("production"));
        assertTrue(validator.isValidContextName("prod-us-east-1"));
        assertTrue(validator.isValidContextName("staging_v2"));
        assertTrue(validator.isValidContextName("cluster123"));
        assertTrue(validator.isValidContextName("1test"));
    }

    @Test
    void testInvalidContextNames() {
        assertFalse(validator.isValidContextName(null));
        assertFalse(validator.isValidContextName(""));
        assertFalse(validator.isValidContextName(" "));
        assertFalse(validator.isValidContextName("my context"));
        assertFalse(validator.isValidContextName("prod@us"));
        assertFalse(validator.isValidContextName("test.cluster"));
        assertFalse(validator.isValidContextName("-prod"));
        assertFalse(validator.isValidContextName("_staging"));
        assertFalse(validator.isValidContextName("a".repeat(254))); // Too long
    }

    @Test
    void testValidateConfigSuccess() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        ValidationResult result = validator.validateConfig(config);

        assertTrue(result.isValid());
    }

    @Test
    void testValidateConfigMissingBootstrapServers() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("security.protocol", "PLAINTEXT")
                .build();

        ValidationResult result = validator.validateConfig(config);

        assertFalse(result.isValid());
        assertTrue(result.getMessage().contains("bootstrap.servers"));
    }

    @Test
    void testValidateConfigEmptyBootstrapServers() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "")
                .build();

        ValidationResult result = validator.validateConfig(config);

        assertFalse(result.isValid());
        assertTrue(result.getMessage().contains("bootstrap.servers"));
    }

    @Test
    void testValidateConfigNull() {
        ValidationResult result = validator.validateConfig(null);

        assertFalse(result.isValid());
        assertTrue(result.getMessage().contains("cannot be null"));
    }
}
