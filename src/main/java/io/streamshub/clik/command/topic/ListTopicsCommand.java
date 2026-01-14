package io.streamshub.clik.command.topic;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.kafka.model.TopicInfo;
import picocli.CommandLine;

@CommandLine.Command(
        name = "list",
        description = "List all Kafka topics"
)
public class ListTopicsCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, yaml, json, name (default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @CommandLine.Option(
            names = {"--internal"},
            description = "Include internal topics (default: false)",
            defaultValue = "false"
    )
    boolean includeInternal;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            Set<String> topicNames = topicService.listTopics(admin, includeInternal);

            if (topicNames.isEmpty()) {
                out().println("No topics found.");
                return 0;
            }

            // Sort topics by name for consistent output order
            List<String> sortedTopics = new ArrayList<>(topicNames);
            sortedTopics.sort(String::compareTo);

            switch (outputFormat.toLowerCase()) {
                case "table":
                    printTable(admin, sortedTopics);
                    break;
                case "name":
                    printNames(sortedTopics);
                    break;
                case "yaml":
                    printYaml(admin, sortedTopics);
                    break;
                case "json":
                    printJson(admin, sortedTopics);
                    break;
                default:
                    err().println("Error: Unknown output format: " + outputFormat);
                    err().println("Valid formats: table, yaml, json, name");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to list topics: " + e.getMessage());
            return 1;
        }
    }

    private void printTable(Admin admin, List<String> topicNames) {
        try {
            Map<String, TopicInfo> topics = topicService.describeTopics(admin, topicNames);
            List<TopicRow> rows = new ArrayList<>();

            for (String name : topicNames) {
                TopicInfo info = topics.get(name);
                if (info != null) {
                    rows.add(new TopicRow(
                            info.name(),
                            String.valueOf(info.partitions()),
                            String.valueOf(info.replicationFactor()),
                            info.internal() ? "yes" : ""
                    ));
                }
            }

            String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, rows, List.of(
                    new Column().header("NAME").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.name),
                    new Column().header("PARTITIONS").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.partitions),
                    new Column().header("REPLICATION").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.replication),
                    new Column().header("INTERNAL").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.CENTER).with(r -> r.internal)
            ));

            out().println(table);
        } catch (Exception e) {
            err().println("Error: Failed to describe topics: " + e.getMessage());
        }
    }

    private void printNames(List<String> topicNames) {
        topicNames.forEach(out()::println);
    }

    private void printYaml(Admin admin, List<String> topicNames) {
        try {
            Map<String, TopicInfo> topics = topicService.describeTopics(admin, topicNames);
            List<Map<String, Object>> topicList = new ArrayList<>();

            for (String name : topicNames) {
                TopicInfo info = topics.get(name);
                if (info != null) {
                    Map<String, Object> data = new LinkedHashMap<>();
                    data.put("name", info.name());
                    data.put("partitions", info.partitions());
                    data.put("replicationFactor", info.replicationFactor());
                    data.put("internal", info.internal());
                    topicList.add(data);
                }
            }

            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().println(yamlMapper.writeValueAsString(topicList));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(Admin admin, List<String> topicNames) {
        try {
            Map<String, TopicInfo> topics = topicService.describeTopics(admin, topicNames);
            List<Map<String, Object>> topicList = new ArrayList<>();

            for (String name : topicNames) {
                TopicInfo info = topics.get(name);
                if (info != null) {
                    Map<String, Object> data = new LinkedHashMap<>();
                    data.put("name", info.name());
                    data.put("partitions", info.partitions());
                    data.put("replicationFactor", info.replicationFactor());
                    data.put("internal", info.internal());
                    topicList.add(data);
                }
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(topicList));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private static record TopicRow(String name, String partitions, String replication, String internal) {}
}
