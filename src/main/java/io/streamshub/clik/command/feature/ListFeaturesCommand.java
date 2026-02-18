package io.streamshub.clik.command.feature;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.ColumnData;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.FeatureService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.FeatureInfo;
import picocli.CommandLine;

@CommandLine.Command(
    name = "list",
    description = "List all cluster features"
)
public class ListFeaturesCommand extends ContextualCommand implements Callable<Integer> {

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
            List<FeatureInfo> features = featureService.listFeatures(admin);

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(features);
                    break;
                case "yaml":
                    printYaml(features);
                    break;
                case "json":
                    printJson(features);
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
            err().println("Error: Failed to list features: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(List<FeatureInfo> features) {
        if (features.isEmpty()) {
            out().println("No features found.");
            return;
        }

        List<ColumnData<FeatureInfo>> columns = new ArrayList<>();
        columns.add(column("FEATURE", HorizontalAlign.LEFT, FeatureInfo::name));
        columns.add(column("STATUS", HorizontalAlign.LEFT, FeatureInfo::status));
        columns.add(column("FINALIZED", HorizontalAlign.RIGHT, this::formatVersionRange));
        columns.add(column("SUPPORTED", HorizontalAlign.RIGHT, this::formatSupportedRange));
        columns.add(column("KAFKA VERSION", HorizontalAlign.LEFT, f ->
            f.kafkaVersion() != null ? f.kafkaVersion() : "-"
        ));

        String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, features, columns);
        out().println(table);
    }

    private String formatVersionRange(FeatureInfo feature) {
        if (feature.finalizedMinVersion() == null || feature.finalizedMaxVersion() == null) {
            return "-";
        }

        if (feature.finalizedMinVersion().equals(feature.finalizedMaxVersion())) {
            return String.valueOf(feature.finalizedMaxVersion());
        }

        return feature.finalizedMinVersion() + "-" + feature.finalizedMaxVersion();
    }

    private String formatSupportedRange(FeatureInfo feature) {
        if (feature.supportedMinVersion() == null || feature.supportedMaxVersion() == null) {
            return "-";
        }

        if (feature.supportedMinVersion().equals(feature.supportedMaxVersion())) {
            return String.valueOf(feature.supportedMaxVersion());
        }

        return feature.supportedMinVersion() + "-" + feature.supportedMaxVersion();
    }

    private void printYaml(List<FeatureInfo> features) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().print(yamlMapper.writeValueAsString(features));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(List<FeatureInfo> features) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(features));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }
}
