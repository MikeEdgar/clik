package io.streamshub.clik.command.feature;

import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.FeatureService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.FeatureInfo;
import picocli.CommandLine;

@CommandLine.Command(
    name = "describe",
    description = "Display detailed information about a specific feature"
)
public class DescribeFeatureCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Parameters(
        index = "0",
        description = "Feature name (e.g., metadata.version)"
    )
    String featureName;

    @CommandLine.Option(
        names = {"-o", "--output"},
        description = "Output format: table, yaml, json (default: table)",
        defaultValue = "table"
    )
    String outputFormat;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    FeatureService featureService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            FeatureInfo feature = featureService.describeFeature(admin, featureName);

            if (feature == null) {
                err().println("Error: Feature \"" + featureName + "\" not found.");
                err().println();
                err().println("Run 'clik feature list' to see available features.");
                return 1;
            }

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(feature);
                    break;
                case "yaml":
                    printYaml(feature);
                    break;
                case "json":
                    printJson(feature);
                    break;
                default:
                    err().println("Error: Unknown output format: " + outputFormat);
                    err().println("Valid formats: table, yaml, json");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to describe feature: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(FeatureInfo feature) {
        out().println("Feature: " + feature.name());
        out().println("Status: " + feature.status());
        out().println();

        if (feature.finalizedMinVersion() != null || feature.finalizedMaxVersion() != null) {
            out().println("Finalized Version Range:");
            if (feature.finalizedMinVersion() != null) {
                out().println("  Min: " + feature.finalizedMinVersion());
            }
            if (feature.finalizedMaxVersion() != null) {
                out().println("  Max: " + feature.finalizedMaxVersion());
            }
            out().println();
        }

        if (feature.supportedMinVersion() != null || feature.supportedMaxVersion() != null) {
            out().println("Supported Version Range:");
            if (feature.supportedMinVersion() != null) {
                out().println("  Min: " + feature.supportedMinVersion());
            }
            if (feature.supportedMaxVersion() != null) {
                out().println("  Max: " + feature.supportedMaxVersion());
            }
            out().println();
        }

        if (feature.kafkaVersion() != null) {
            out().println("Kafka Version: " + feature.kafkaVersion());
        }
    }

    private void printYaml(FeatureInfo feature) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().print(yamlMapper.writeValueAsString(feature));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(FeatureInfo feature) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(feature));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }
}
