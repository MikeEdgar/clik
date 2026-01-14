package io.streamshub.clik.command.cluster;

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
import io.streamshub.clik.kafka.ClusterService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.ClusterInfo;
import io.streamshub.clik.kafka.model.NodeInfo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "describe",
        description = "Display detailed information about the Kafka cluster"
)
public class DescribeClusterCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    ClusterService clusterService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            ClusterInfo cluster = clusterService.describeCluster(admin);

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(cluster);
                    break;
                case "yaml":
                    printYaml(cluster);
                    break;
                case "json":
                    printJson(cluster);
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
            err().println("Error: Failed to describe cluster: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(ClusterInfo cluster) {
        // Cluster summary
        out().println("Cluster ID: " + cluster.clusterId());
        out().println("Feature Level: " + cluster.featureLevel());
        out().println("Controller ID: " + cluster.controllerId());
        out().println("Nodes: " + cluster.nodes().size());

        // Quorum metadata section (KRaft only)
        if (cluster.quorumInfo() != null) {
            out().println();
            out().println("Metadata Quorum:");
            out().println("  Leader ID: " + cluster.quorumInfo().leaderId());
            out().println("  Controller Epoch: " + cluster.quorumInfo().leaderEpoch());
            out().println("  High Watermark: " + cluster.quorumInfo().highWatermark());
        }

        out().println();

        // Nodes table
        if (!cluster.nodes().isEmpty()) {
            List<ColumnData<NodeInfo>> columns = new ArrayList<>();
            columns.add(column("ID", HorizontalAlign.RIGHT, node -> String.valueOf(node.id())));
            columns.add(column("HOST", HorizontalAlign.LEFT, node -> node.host() != null ? node.host() : "-"));
            columns.add(column("PORT", HorizontalAlign.LEFT, node -> node.port() > -1 ? String.valueOf(node.port()) : "-"));
            columns.add(column("RACK", HorizontalAlign.LEFT, node -> node.rack() != null ? node.rack() : "-"));
            columns.add(column("ROLE", HorizontalAlign.LEFT, node -> node.role().toString()));

            if (cluster.quorumInfo() != null) {
                columns.add(column("QUORUM ROLE", HorizontalAlign.LEFT, NodeInfo::quorumRoleDisplay));
            }

            String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, cluster.nodes(), columns);

            out().println("Cluster Nodes:");
            out().println(table);
        }
    }

    private void printYaml(ClusterInfo cluster) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().print(yamlMapper.writeValueAsString(cluster));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(ClusterInfo cluster) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(cluster));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }
}
