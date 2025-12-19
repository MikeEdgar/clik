package io.streamshub.clik.command.topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.kafka.model.PartitionInfo;
import io.streamshub.clik.kafka.model.TopicInfo;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "describe",
        description = "Display detailed information about a topic"
)
public class DescribeTopicCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String name;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient()) {
            TopicInfo topic = topicService.describeTopic(admin, name);

            if (topic == null) {
                System.err.println("Error: Topic \"" + name + "\" not found.");
                System.err.println();
                System.err.println("Run 'clik topic list' to see available topics.");
                return 1;
            }

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(topic);
                    break;
                case "yaml":
                    printYaml(topic);
                    break;
                case "json":
                    printJson(topic);
                    break;
                default:
                    System.err.println("Error: Unknown output format: " + outputFormat);
                    System.err.println("Valid formats: table, yaml, json");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            // Check if it's an unknown topic exception
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                System.err.println("Error: Topic \"" + name + "\" not found.");
                System.err.println();
                System.err.println("Run 'clik topic list' to see available topics.");
                return 1;
            }
            System.err.println("Error: Failed to describe topic: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(TopicInfo topic) {
        System.out.println("Topic: " + topic.getName());
        System.out.println("Partitions: " + topic.getPartitions());
        System.out.println("Replication Factor: " + topic.getReplicationFactor());
        System.out.println("Internal: " + (topic.isInternal() ? "yes" : "no"));
        System.out.println();

        if (!topic.getConfig().isEmpty()) {
            System.out.println("Configuration:");
            topic.getConfig().entrySet().stream()
                    .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                    .forEach(entry -> System.out.println("  " + entry.getKey() + " = " + entry.getValue()));
            System.out.println();
        }

        if (topic.getPartitionDetails() != null && !topic.getPartitionDetails().isEmpty()) {
            System.out.println("Partition Details:");
            List<PartitionRow> rows = new ArrayList<>();
            for (PartitionInfo partition : topic.getPartitionDetails()) {
                rows.add(new PartitionRow(
                        String.valueOf(partition.getId()),
                        String.valueOf(partition.getLeader()),
                        partition.getReplicas().toString(),
                        partition.getIsr().toString()
                ));
            }

            String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, rows, List.of(
                    new Column().header("PARTITION").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.partition),
                    new Column().header("LEADER").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.leader),
                    new Column().header("REPLICAS").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.replicas),
                    new Column().header("ISR").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.isr)
            ));

            System.out.println(table);
        }
    }

    private void printYaml(TopicInfo topic) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            System.out.print(yamlMapper.writeValueAsString(topic));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(TopicInfo topic) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            System.out.println(jsonMapper.writeValueAsString(topic));
        } catch (Exception e) {
            System.err.println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private static class PartitionRow {
        final String partition;
        final String leader;
        final String replicas;
        final String isr;

        PartitionRow(String partition, String leader, String replicas, String isr) {
            this.partition = partition;
            this.leader = leader;
            this.replicas = replicas;
            this.isr = isr;
        }
    }
}
