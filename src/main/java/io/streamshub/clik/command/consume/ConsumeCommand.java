package io.streamshub.clik.command.consume;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.ColumnData;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.kafka.ConfigCandidates;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.KafkaRecord;
import io.streamshub.clik.support.LifecycleHandler;
import io.streamshub.clik.support.OutputFormatter;
import picocli.CommandLine;

@CommandLine.Command(
        name = "consume",
        description = "Consume messages from a Kafka topic"
)
public class ConsumeCommand extends BaseCommand implements Callable<Integer> {

    private static final String OUTPUT_TABLE = "table";
    private static final String OUTPUT_JSON = "json";
    private static final String OUTPUT_YAML = "yaml";

    @Inject
    Logger logger;

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String topic;

    @CommandLine.Option(
            names = {"-g", "--group"},
            description = "Consumer group ID"
    )
    String groupId;

    @CommandLine.Option(
            names = {"-f", "--follow"},
            description = "Continuous mode (consume until interrupted)"
    )
    boolean follow;

    @CommandLine.Option(
            names = {"--from-beginning"},
            description = "Start from earliest offset"
    )
    boolean fromBeginning;

    @CommandLine.Option(
            names = {"--from-end"},
            description = "Start from latest offset"
    )
    boolean fromEnd;

    @CommandLine.Option(
            names = {"--from-offset"},
            description = "Start from specific offset (requires --partition)"
    )
    Long offset;

    @CommandLine.Option(
            names = {"-p", "--partition"},
            description = "Consume from specific partition only"
    )
    Integer partition;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output format: table, json, yaml, or custom format string (e.g., '%k=%v' or '%v', default: table)",
            defaultValue = "table"
    )
    String outputFormat;

    @CommandLine.Option(
            names = {"--max-messages"},
            description = "Maximum number of messages to consume"
    )
    Integer maxMessages;

    @CommandLine.Option(
            names = {"--timeout"},
            description = "Timeout in milliseconds for one-time consumption (default: 5000)",
            defaultValue = "5000"
    )
    long timeout;

    @CommandLine.Option(
            names = {"--property", "-P"},
            description = "Consumer client properties to override those in the context (repeatable, format: key=value)",
            paramLabel = "property",
            completionCandidates = ConfigCandidates.Consumer.class
    )
    Map<String, String> properties = new HashMap<>();

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    LifecycleHandler lifecycle;

    @Override
    public Integer call() {
        // Validate offset control options
        int offsetOptions = 0;

        if (fromBeginning) {
            offsetOptions++;
        }
        if (fromEnd) {
            offsetOptions++;
        }
        if (offset != null) {
            offsetOptions++;
        }

        if (offsetOptions > 1) {
            err().println("Error: Only one of --from-beginning, --from-end, or --from-offset can be specified");
            return 1;
        }

        // Consumer cannot subscribe to a group using a specific partition
        if (groupId != null && partition != null) {
            err().println("Error: --groupId cannot be used with --partition");
            return 1;
        }

        // Validate offset requires partition
        if (offset != null && partition == null) {
            err().println("Error: --from-offset requires --partition to be specified");
            return 1;
        }

        // Validate output format
        if (!validOutputFormat()) {
            err().println("Error: Invalid output format: " + outputFormat);
            err().println("Valid formats: table, json, yaml, or a format string (e.g., '%k=%v' or '%v')");
            return 1;
        }

        return lifecycle.supply(this::execute).join();
    }

    private int execute() {
        try (Consumer<byte[], byte[]> consumer = clientFactory.createConsumer(properties, groupId)) {
            configureConsumer(consumer);

            if (follow) {
                return consumeContinuously(consumer);
            } else {
                List<KafkaRecord> messages = consumeOnce(consumer);
                if (messages.isEmpty()) {
                    err().println("No messages consumed");
                } else {
                    printMessages(messages);
                }
                return 0;
            }
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to consume messages: " + e.getMessage());
            return 1;
        }
    }

    private boolean validOutputFormat() {
        // Check if it's a predefined format first
        if (Stream.of(OUTPUT_TABLE, OUTPUT_JSON, OUTPUT_YAML)
                .anyMatch(outputFormat.toLowerCase()::equals)) {
            return true;
        }

        // Otherwise try to parse as format string
        // Format strings must contain at least one % placeholder
        if (outputFormat.contains("%")) {
            try {
                OutputFormatter.withFormat(outputFormat);
                return true;
            } catch (IllegalArgumentException _) {
                // Invalid format string - validation will fail
                return false;
            }
        }

        // Not a predefined format and not a valid format string
        return false;
    }

    private void configureConsumer(Consumer<byte[], byte[]> consumer) {
        if (partition != null) {
            assignSinglePartition(consumer);
        } else if (groupId != null) {
            subscribeAllPartitions(consumer);
        } else {
            assignAllPartitions(consumer);
        }
    }

    private void assignSinglePartition(Consumer<byte[], byte[]> consumer) {
        // Single partition mode
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        var assignment = Collections.singleton(topicPartition);
        consumer.assign(assignment);

        if (offset != null) {
            consumer.seek(topicPartition, offset);
        } else if (fromBeginning) {
            consumer.seekToBeginning(assignment);
        } else if (fromEnd) {
            consumer.seekToEnd(assignment);
        }
    }

    private void assignAllPartitions(Consumer<byte[], byte[]> consumer) {
        // All partitions mode
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("Topic not found or has no partitions: " + topic);
        }

        Set<TopicPartition> assignment = partitions.stream()
            .map(p -> new TopicPartition(topic, p.partition()))
            .collect(Collectors.toSet());

        consumer.assign(assignment);

        if (fromBeginning) {
            consumer.seekToBeginning(consumer.assignment());
        } else if (fromEnd) {
            // Default for standalone (no group): from end
            // Default for named group: let Kafka manage offsets
            consumer.seekToEnd(consumer.assignment());
        }
    }

    private void subscribeAllPartitions(Consumer<byte[], byte[]> consumer) {
        AtomicBoolean initial = new AtomicBoolean(true);

        consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Not interested
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (initial.compareAndSet(true, false)) {
                    logger.infof("Member %s in group %s received assignment: %s",
                            consumer.groupMetadata().memberId(),
                            consumer.groupMetadata().groupId(),
                            consumer.assignment());

                    if (fromBeginning) {
                        consumer.seekToBeginning(partitions);
                    } else if (fromEnd) {
                        consumer.seekToEnd(partitions);
                    }
                }
            }
        });
    }

    private List<KafkaRecord> consumeOnce(Consumer<byte[], byte[]> consumer) {
        List<KafkaRecord> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeout) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                messages.add(KafkaRecord.from(rec));

                if (maxMessages != null && messages.size() >= maxMessages) {
                    return messages;
                }
            }

            if (!records.isEmpty()) {
                // Reset timeout when we receive messages
                startTime = System.currentTimeMillis();
            }
        }

        return messages;
    }

    private int consumeContinuously(Consumer<byte[], byte[]> consumer) {
        int count = 0;

        // Check if we're using a format string and parse it once for efficiency
        boolean isPredefinedFormat = Stream.of(OUTPUT_TABLE, OUTPUT_JSON, OUTPUT_YAML)
                .anyMatch(outputFormat::equalsIgnoreCase);

        OutputFormatter formatter = !isPredefinedFormat ? OutputFormatter.withFormat(outputFormat) : null;

        while (lifecycle.isRunning() && (maxMessages == null || count < maxMessages)) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                KafkaRecord msg = KafkaRecord.from(rec);

                if (outputFormat.equalsIgnoreCase(OUTPUT_TABLE)) {
                    printTableRow(msg);
                } else if (formatter != null) {
                    // Format string mode - use pre-parsed formatter
                    out().println(formatter.format(msg));
                } else {
                    // Predefined format (json, yaml, value)
                    printMessages(List.of(msg));
                }

                count++;
            }
        }

        err().println("\n" + count + " messages consumed");
        return 0;
    }

    private void printMessages(List<KafkaRecord> messages) {
        switch (outputFormat.toLowerCase()) {
            case OUTPUT_TABLE:
                printTable(messages);
                break;
            case OUTPUT_JSON:
                printJson(messages);
                break;
            case OUTPUT_YAML:
                printYaml(messages);
                break;
            default:
                printFormatString(messages);
                break;
        }
    }

    private void printFormatString(List<KafkaRecord> messages) {
        OutputFormatter formatter = OutputFormatter.withFormat(outputFormat);
        for (KafkaRecord msg : messages) {
            out().println(formatter.format(msg));
        }
    }

    private void printTable(List<KafkaRecord> messages) {
        String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, messages, List.of(
                column("PARTITION", HorizontalAlign.RIGHT, msg -> String.valueOf(msg.partition())),
                column("OFFSET", HorizontalAlign.RIGHT, msg -> String.valueOf(msg.offset())),
                column("KEY", HorizontalAlign.LEFT, msg -> msg.keyString("")),
                column("VALUE", HorizontalAlign.LEFT, msg -> msg.valueString(""))
        ));

        out().println(table);
    }

    private static <T> ColumnData<T> column(String name, HorizontalAlign dataAlign, Function<T, String> data) {
        return new Column().header(name).headerAlign(HorizontalAlign.LEFT).dataAlign(dataAlign).with(data);
    }

    private void printTableRow(KafkaRecord msg) {
        out().printf("%d\t%d\t%s\t%s%n",
                msg.partition(),
                msg.offset(),
                msg.keyString(""),
                msg.valueString(""));
    }

    private void printJson(List<KafkaRecord> messages) {
        ObjectMapper jsonMapper = new ObjectMapper()
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

        try {
            for (KafkaRecord msg : messages) {
                printStructuredMessage(jsonMapper, msg);
                out().println();
            }
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private void printYaml(List<KafkaRecord> messages) {
        YAMLFactory yamlFactory = YAMLFactory.builder()
                .enable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
        ObjectMapper yamlMapper = new ObjectMapper(yamlFactory)
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

        try {
            for (KafkaRecord msg : messages) {
                printStructuredMessage(yamlMapper, msg);
                if (follow) {
                    // Write the document end marker when streaming
                    out().println("...");
                }
            }
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printStructuredMessage(ObjectMapper mapper, KafkaRecord msg) throws IOException {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("partition", msg.partition());
        data.put("offset", msg.offset());
        data.put("key", msg.keyString(null));
        data.put("value", msg.valueString(null));
        data.put("timestamp", msg.timestamp());
        data.put("headers", convertHeadersToMapList(msg.headers()));
        mapper.writeValue(out(), data);
    }

    /**
     * Convert headers to a list of maps for JSON/YAML serialization.
     * Each header becomes a map with "key" and "value" properties.
     */
    private List<Map<String, String>> convertHeadersToMapList(List<KafkaRecord.Header> headers) {
        List<Map<String, String>> headerList = new ArrayList<>(headers.size());
        for (var header : headers) {
            Map<String, String> headerMap = new LinkedHashMap<>();
            headerMap.put("key", header.key());
            headerMap.put("value", header.valueString(null));
            headerList.add(headerMap);
        }
        return headerList;
    }
}
