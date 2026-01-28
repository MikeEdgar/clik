package io.streamshub.clik.command.topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.ColumnData;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.command.topic.options.OffsetsOption;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.kafka.model.PartitionInfo;
import io.streamshub.clik.kafka.model.TopicInfo;
import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;

@CommandLine.Command(
        name = "describe",
        description = "Display detailed information about a topic"
)
public class DescribeTopicCommand extends ContextualCommand implements Callable<Integer> {

    private static final Logger LOGGER = Logger.getLogger(DescribeTopicCommand.class);

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name",
            completionCandidates = NameCandidate.Topic.class
    )
    String name;

    @CommandLine.Option(
            names = {"--offsets"},
            description = "Comma-separated list of offsets to fetch",
            paramLabel = "<offset-spec>",
            arity = "1",
            split = ",",
            completionCandidates = OffsetsOption.Candidates.class,
            converter = OffsetsOption.Converter.class
    )
    List<OffsetsOption> offsets = new ArrayList<>();

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
        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            TopicInfo topic = topicService.describeTopic(admin, name, offsets);

            if (topic == null) {
                err().println("Error: Topic \"" + name + "\" not found.");
                err().println();
                err().println("Run 'clik topic list' to see available topics.");
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
                    err().println("Error: Unknown output format: " + outputFormat);
                    err().println("Valid formats: table, yaml, json");
                    return 1;
            }

            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            // Check if it's an unknown topic exception
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                err().println("Error: Topic \"" + name + "\" not found.");
                err().println();
                err().println("Run 'clik topic list' to see available topics.");
                return 1;
            }
            err().println("Error: Failed to describe topic: " + e.getMessage());
            LOGGER.warn("Error: Failed to describe topic", e);
            return 1;
        }
    }

    private void printTable(TopicInfo topic) {
        out().println("Topic: " + topic.name());
        out().println("Partitions: " + topic.partitions());
        out().println("Replication Factor: " + topic.replicationFactor());
        out().println("Internal: " + (topic.internal() ? "yes" : "no"));
        out().println();

        if (!topic.config().isEmpty()) {
            out().println("Configuration:");
            topic.config().entrySet().stream()
                    .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                    .forEach(entry -> out().println("  " + entry.getKey() + " = " + entry.getValue()));
            out().println();
        }

        if (topic.partitionDetails() != null && !topic.partitionDetails().isEmpty()) {
            out().println("Partition Details:");
            List<ColumnData<PartitionInfo>> columns = new ArrayList<>();
            columns.add(column("PARTITION", HorizontalAlign.RIGHT, p -> String.valueOf(p.id())));
            columns.add(column("LEADER", HorizontalAlign.RIGHT, p -> String.valueOf(p.leader())));
            columns.add(column("REPLICAS", HorizontalAlign.LEFT, p -> p.replicas().toString()));
            columns.add(column("ISR", HorizontalAlign.LEFT, p -> p.isr().toString()));

            for (int i = 0; i < offsets.size(); i++) {
                int o = i;
                columns.add(column(
                        "OFFSET (" + offsets.get(i).option().toUpperCase(Locale.ROOT) + ')',
                        HorizontalAlign.RIGHT,
                        p -> String.valueOf(p.offsets().get(o).value())
                ));
            }

            out().println(AsciiTable.getTable(AsciiTable.NO_BORDERS, topic.partitionDetails(), columns));
        }
    }

    private void printYaml(TopicInfo topic) {
        try {
            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().print(yamlMapper.writeValueAsString(topic));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printJson(TopicInfo topic) {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(topic));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }
}
