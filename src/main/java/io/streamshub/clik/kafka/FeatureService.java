package io.streamshub.clik.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.server.common.MetadataVersion;

import io.streamshub.clik.kafka.model.FeatureInfo;

@ApplicationScoped
public class FeatureService {

    /**
     * List all features (both finalized and supported)
     *
     * @param admin Kafka Admin client
     * @return List of all features with their version ranges
     */
    public List<FeatureInfo> listFeatures(Admin admin) throws Exception {
        var describeResult = admin.describeFeatures();
        var metadata = describeResult.featureMetadata()
            .toCompletionStage()
            .toCompletableFuture()
            .join();

        Map<String, FinalizedVersionRange> finalizedFeatures = metadata.finalizedFeatures();
        Map<String, SupportedVersionRange> supportedFeatures = metadata.supportedFeatures();

        // Merge features from both maps
        Map<String, FeatureInfo> featureMap = new HashMap<>();

        // Add finalized features
        for (Map.Entry<String, FinalizedVersionRange> entry : finalizedFeatures.entrySet()) {
            String name = entry.getKey();
            FinalizedVersionRange finalized = entry.getValue();
            SupportedVersionRange supported = supportedFeatures.get(name);
            featureMap.put(name, buildFeatureInfo(name, finalized, supported));
        }

        // Add supported-only features (not finalized)
        for (Map.Entry<String, SupportedVersionRange> entry : supportedFeatures.entrySet()) {
            String name = entry.getKey();
            if (!featureMap.containsKey(name)) {
                featureMap.put(name, buildFeatureInfo(name, null, entry.getValue()));
            }
        }

        return new ArrayList<>(featureMap.values());
    }

    /**
     * Describe a specific feature by name
     *
     * @param admin Kafka Admin client
     * @param featureName Feature name to describe
     * @return FeatureInfo or null if feature not found
     */
    public FeatureInfo describeFeature(Admin admin, String featureName) throws Exception {
        var describeResult = admin.describeFeatures();
        var metadata = describeResult.featureMetadata()
            .toCompletionStage()
            .toCompletableFuture()
            .join();

        Map<String, FinalizedVersionRange> finalizedFeatures = metadata.finalizedFeatures();
        Map<String, SupportedVersionRange> supportedFeatures = metadata.supportedFeatures();

        FinalizedVersionRange finalized = finalizedFeatures.get(featureName);
        SupportedVersionRange supported = supportedFeatures.get(featureName);

        if (finalized == null && supported == null) {
            return null;
        }

        return buildFeatureInfo(featureName, finalized, supported);
    }

    /**
     * Update a feature to a specific version level
     *
     * @param admin Kafka Admin client
     * @param featureName Feature name to update
     * @param targetVersion Target version level
     * @param allowDowngrade Whether to allow downgrade
     */
    public void updateFeature(Admin admin, String featureName, short targetVersion, boolean allowDowngrade) throws Exception {
        Map<String, FeatureUpdate> updates = Map.of(
            featureName,
            new FeatureUpdate(targetVersion, allowDowngrade ? FeatureUpdate.UpgradeType.SAFE_DOWNGRADE : FeatureUpdate.UpgradeType.UPGRADE)
        );

        admin.updateFeatures(updates, new UpdateFeaturesOptions())
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    }

    /**
     * Disable a feature (reset to version 0)
     *
     * @param admin Kafka Admin client
     * @param featureName Feature name to disable
     */
    public void disableFeature(Admin admin, String featureName) throws Exception {
        Map<String, FeatureUpdate> updates = Map.of(
            featureName,
            new FeatureUpdate((short) 0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)
        );

        admin.updateFeatures(updates, new UpdateFeaturesOptions())
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join();
    }

    /**
     * Build FeatureInfo from finalized and supported version ranges
     */
    private FeatureInfo buildFeatureInfo(String name, FinalizedVersionRange finalized, SupportedVersionRange supported) {
        var builder = FeatureInfo.builder().name(name);

        if (finalized != null) {
            builder.finalizedMinVersion(finalized.minVersionLevel())
                   .finalizedMaxVersion(finalized.maxVersionLevel())
                   .status("FINALIZED");
        } else {
            builder.status("SUPPORTED_ONLY");
        }

        if (supported != null) {
            builder.supportedMinVersion(supported.minVersion())
                   .supportedMaxVersion(supported.maxVersion());
        }

        // Map metadata.version to Kafka version
        if (MetadataVersion.FEATURE_NAME.equals(name) && finalized != null) {
            builder.kafkaVersion(mapMetadataVersionToKafkaVersion(finalized.maxVersionLevel()));
        }

        return builder.build();
    }

    /**
     * Map metadata.version level to Kafka version string
     * Reuses logic from ClusterService
     */
    private String mapMetadataVersionToKafkaVersion(short versionLevel) {
        try {
            return MetadataVersion.fromFeatureLevel(versionLevel).shortVersion();
        } catch (IllegalArgumentException _) {
            if (versionLevel < MetadataVersion.MINIMUM_VERSION.featureLevel()) {
                return "Unknown (<%s)".formatted(MetadataVersion.MINIMUM_VERSION.shortVersion());
            }
            return "Unknown (>%s)".formatted(MetadataVersion.latestTesting().shortVersion());
        }
    }
}
