package io.streamshub.clik.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.kafka.model.FeatureInfo;
import io.streamshub.clik.test.ClikTestBase;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class FeatureServiceTest extends ClikTestBase {

    @Inject
    FeatureService featureService;

    @Test
    void testListFeatures() throws Exception {
        List<FeatureInfo> features = featureService.listFeatures(admin());
        assertNotNull(features);
        assertFalse(features.isEmpty());

        // Verify metadata.version is present
        assertTrue(features.stream()
            .anyMatch(f -> "metadata.version".equals(f.name())),
            "metadata.version should be in feature list");
    }

    @Test
    void testListFeaturesContainsBothFinalizedAndSupported() throws Exception {
        List<FeatureInfo> features = featureService.listFeatures(admin());

        // Should have at least one finalized feature
        assertTrue(features.stream()
            .anyMatch(f -> "FINALIZED".equals(f.status())),
            "Should have at least one finalized feature");

        // Should have version ranges
        assertTrue(features.stream()
            .anyMatch(f -> f.supportedMaxVersion() != null),
            "Should have supported version ranges");
    }

    @Test
    void testDescribeFeatureFound() throws Exception {
        FeatureInfo feature = featureService.describeFeature(admin(), "metadata.version");

        assertNotNull(feature);
        assertEquals("metadata.version", feature.name());
        assertNotNull(feature.kafkaVersion(), "metadata.version should have Kafka version mapping");
        assertEquals("FINALIZED", feature.status());
        assertNotNull(feature.finalizedMaxVersion());
        assertNotNull(feature.supportedMaxVersion());
    }

    @Test
    void testDescribeFeatureNotFound() throws Exception {
        FeatureInfo feature = featureService.describeFeature(admin(), "non.existent.feature");
        assertNull(feature, "Non-existent feature should return null");
    }

    @Test
    void testKafkaVersionMapping() throws Exception {
        FeatureInfo feature = featureService.describeFeature(admin(), "metadata.version");

        assertNotNull(feature);
        assertNotNull(feature.kafkaVersion());

        // Version should match pattern: "X.Y" or "Unknown (...)"
        String version = feature.kafkaVersion();
        assertTrue(
            version.matches("\\d+\\.\\d+") || version.startsWith("Unknown"),
            "Kafka version should match 'X.Y' pattern or start with 'Unknown', got: " + version
        );
    }

    @Test
    void testFeatureInfoBuilder() {
        FeatureInfo feature = FeatureInfo.builder()
            .name("test.feature")
            .finalizedMinVersion((short) 1)
            .finalizedMaxVersion((short) 5)
            .supportedMinVersion((short) 1)
            .supportedMaxVersion((short) 10)
            .status("FINALIZED")
            .kafkaVersion("4.2")
            .build();

        assertEquals("test.feature", feature.name());
        assertEquals((short) 1, feature.finalizedMinVersion());
        assertEquals((short) 5, feature.finalizedMaxVersion());
        assertEquals((short) 1, feature.supportedMinVersion());
        assertEquals((short) 10, feature.supportedMaxVersion());
        assertEquals("FINALIZED", feature.status());
        assertEquals("4.2", feature.kafkaVersion());
    }
}
