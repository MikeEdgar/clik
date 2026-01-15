package io.streamshub.clik.command.consume;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
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

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.ConfigCandidates;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.KafkaRecord;
import io.streamshub.clik.support.LifecycleHandler;
import io.streamshub.clik.support.OutputFormatter;
import picocli.CommandLine;
import picocli.CommandLine.ITypeConverter;

@CommandLine.Command(
        name = "consume",
        description = "Consume messages from a Kafka topic"
)
public class ConsumeCommand extends ContextualCommand implements Callable<Integer> {

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
            names = {"--from-datetime"},
            description = """
                    Start from a specific date/time or relative duration from the current time \
                    in ISO-8601 format. For example, an absolute datetime '2026-01-01T00:00:00Z' \
                    or 'PT24H' for messages produced in the last 24 hours.""",
            converter = DateTimeConverter.class
    )
    Instant fromDatetime;

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

    private static final class DateTimeConverter implements ITypeConverter<Instant> {
        @Override
        public Instant convert(String value) throws Exception {
            return parseFromDatetime(() -> OffsetDateTime.parse(value).toInstant())
                    .or(() -> parseFromDatetime(() -> Instant.now().minus(Duration.parse(value))))
                    .orElseThrow(() -> new IllegalArgumentException("--from-datetime is not a valid datetime or duration: " + value));
        }

        private static Optional<Instant> parseFromDatetime(Supplier<Instant> source) {
            try {
                return Optional.of(source.get());
            } catch (DateTimeParseException _) {
                return Optional.empty();
            }
        }
    }

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
        if (fromDatetime != null) {
            offsetOptions++;
        }

        if (offsetOptions > 1) {
            err().println("Error: Only one of --from-beginning, --from-end, --from-offset, or --from-datetime can be specified");
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
        try (Consumer<byte[], byte[]> consumer = clientFactory.createConsumer(contextName, properties, groupId)) {
            configureConsumer(consumer);
            return consume(consumer);
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
        } else {
            seek(consumer, assignment);
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
        seek(consumer, assignment);
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
                            partitions);

                    seek(consumer, partitions);
                }
            }
        });
    }

    private void seek(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
        if (fromBeginning) {
            consumer.seekToBeginning(assignment);
        } else if (fromEnd) {
            consumer.seekToEnd(assignment);
        } else if (fromDatetime != null) {
            long epochMs = fromDatetime.toEpochMilli();
            var times = assignment.stream().collect(Collectors.toMap(Function.identity(), _ -> epochMs));
            var endOffsets = consumer.endOffsets(assignment);

            consumer.offsetsForTimes(times)
                    .forEach((p, result) -> {
                        if (result != null) {
                            consumer.seek(p, result.offset());
                        } else {
                            consumer.seek(p, endOffsets.get(p));
                        }
                    });
        }
    }

    private int consume(Consumer<byte[], byte[]> consumer) {
        long count = 0;

        ObjectMapper mapper = null;
        OutputFormatter formatter = null;

        // Check if we're using a format string and parse it once for efficiency
        switch (outputFormat.toLowerCase()) {
            case OUTPUT_TABLE:
                break;
            case OUTPUT_JSON:
                mapper = jsonMapper();
                break;
            case OUTPUT_YAML:
                mapper = yamlMapper();
                break;
            default:
                formatter = OutputFormatter.withFormat(outputFormat);
                break;
        }

        long startTime = System.currentTimeMillis();

        while (lifecycle.isRunning() && continueConsuming(consumer, startTime, count)) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                count++;
                KafkaRecord msg = KafkaRecord.from(rec);

                switch (outputFormat.toLowerCase()) {
                    case OUTPUT_TABLE:
                        printTableRow(msg, count);
                        break;
                    case OUTPUT_JSON:
                        printStructuredMessage(mapper, msg);
                        break;
                    case OUTPUT_YAML:
                        printStructuredMessage(mapper, msg);
                        break;
                    default:
                        // Format string mode - use pre-parsed formatter
                        out().println(formatter.format(msg));
                        break;
                }

                if (maxMessagesConsumed(count)) {
                    break;
                }
            }

            if (!records.isEmpty()) {
                // Reset timeout when we receive messages
                startTime = System.currentTimeMillis();
            }
        }

        if (count > 0) {
            err().println("\n" + count + " messages consumed");
        } else {
            err().println("No messages consumed");
        }

        return 0;
    }

    private boolean continueConsuming(Consumer<byte[], byte[]> consumer, long startTime, long consumedCount) {
        if (maxMessagesConsumed(consumedCount)) {
            return false;
        }

        if (follow) {
            return true;
        } else {
            return (System.currentTimeMillis() - startTime < timeout) && !fullyConsumed(consumer);
        }
    }

    private boolean maxMessagesConsumed(long consumedCount) {
        return maxMessages != null && consumedCount >= maxMessages;
    }

    private boolean fullyConsumed(Consumer<?, ?> consumer) {
        var assignment = consumer.assignment();

        if (assignment.isEmpty()) {
            // Consumer group that has not received the assignment yet.
            return false;
        }

        var endOffsets = consumer.endOffsets(assignment);

        return endOffsets.entrySet().stream().allMatch(e -> {
            var endOffset = e.getValue();
            var position = consumer.position(e.getKey());
            // This should be correct for either consumer isolation level
            return position >= endOffset;
        });
    }

    private void printTableRow(KafkaRecord msg, long count) {
        if (count == 1) {
            out().println("PARTITION\tOFFSET\tKEY\tVALUE");
        }

        out().printf("%d\t%d\t%s\t%s%n",
                msg.partition(),
                msg.offset(),
                msg.keyString(""),
                msg.valueString(""));
    }

    private ObjectMapper jsonMapper() {
        return new ObjectMapper()
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }

    private ObjectMapper yamlMapper() {
        YAMLFactory yamlFactory = YAMLFactory.builder()
                .enable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
        return new ObjectMapper(yamlFactory)
                .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    }

    private void printStructuredMessage(ObjectMapper mapper, KafkaRecord msg) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("partition", msg.partition());
        data.put("offset", msg.offset());
        data.put("key", msg.keyString(null));
        data.put("value", msg.valueString(null));
        data.put("timestamp", msg.timestamp());
        data.put("headers", convertHeadersToMapList(msg.headers()));

        try {
            mapper.writeValue(out(), data);
        } catch (Exception e) {
            err().println("Error: Failed to generate output: " + e.getMessage());
        }
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
