package io.streamshub.clik.command.consume;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;

import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.KafkaRecord;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

@CommandLine.Command(
        name = "consume",
        description = "Consume messages from a Kafka topic"
)
public class ConsumeCommand implements Callable<Integer> {

    @CommandLine.Spec
    CommandSpec commandSpec;

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String topic;

    @CommandLine.Option(
            names = {"-g", "--group"},
            description = "Consumer group ID (default: generated unique ID)"
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
            description = "Output format: table, json, yaml, value (default: table)",
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

    @Inject
    KafkaClientFactory clientFactory;

    private PrintWriter out() {
        return commandSpec.commandLine().getOut();
    }

    private PrintWriter err() {
        return commandSpec.commandLine().getErr();
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

        if (offsetOptions > 1) {
            err().println("Error: Only one of --from-beginning, --from-end, or --from-offset can be specified");
            return 1;
        }

        // Validate offset requires partition
        if (offset != null && partition == null) {
            err().println("Error: --from-offset requires --partition to be specified");
            return 1;
        }

        // Validate output format
        String format = outputFormat.toLowerCase();
        if (!format.equals("table") && !format.equals("json") &&
            !format.equals("yaml") && !format.equals("value")) {
            err().println("Error: Invalid output format: " + outputFormat);
            err().println("Valid formats: table, json, yaml, value");
            return 1;
        }

        // Generate group ID if not specified
        String consumerGroupId = groupId != null ? groupId :
                "clik-consumer-" + UUID.randomUUID().toString();

        try (Consumer<byte[], byte[]> consumer = clientFactory.createConsumer(consumerGroupId)) {
            configureConsumer(consumer);

            if (follow) {
                return consumeContinuously(consumer);
            } else {
                List<KafkaRecord> messages = consumeOnce(consumer);
                if (messages.isEmpty()) {
                    if (!format.equals("value")) {
                        out().println("No messages consumed");
                    }
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

    private void configureConsumer(Consumer<byte[], byte[]> consumer) {
        if (partition != null) {
            // Single partition mode
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(Collections.singleton(tp));

            if (offset != null) {
                consumer.seek(tp, offset);
            } else if (fromBeginning) {
                consumer.seekToBeginning(Collections.singleton(tp));
            } else if (fromEnd) {
                consumer.seekToEnd(Collections.singleton(tp));
            }
        } else {
            // All partitions mode
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalArgumentException("Topic not found or has no partitions: " + topic);
            }

            List<TopicPartition> tps = partitions.stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();

            consumer.assign(tps);

            if (fromBeginning) {
                consumer.seekToBeginning(tps);
            } else if (fromEnd || groupId == null) {
                // Default for standalone (no group): from end
                // Default for named group: let Kafka manage offsets
                consumer.seekToEnd(tps);
            }
        }
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
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger count = new AtomicInteger(0);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        while (running.get()) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                printMessage(KafkaRecord.from(rec));

                if (maxMessages != null && count.incrementAndGet() >= maxMessages) {
                    running.set(false);
                    break;
                }
            }
        }

        if (!outputFormat.equalsIgnoreCase("value")) {
            err().println("\n" + count.get() + " messages consumed");
        }
        return 0;
    }

    private void printMessage(KafkaRecord message) {
        switch (outputFormat.toLowerCase()) {
            case "table":
                printTableRow(message);
                break;
            case "json":
                printJsonMessage(message);
                break;
            case "yaml":
                printYamlMessage(message);
                break;
            case "value":
                out().println(message.valueString(null));
                break;
        }
    }

    private void printMessages(List<KafkaRecord> messages) {
        switch (outputFormat.toLowerCase()) {
            case "table":
                printTable(messages);
                break;
            case "json":
                printJson(messages);
                break;
            case "yaml":
                printYaml(messages);
                break;
            case "value":
                messages.forEach(m -> out().println(m.valueString(null)));
                break;
        }
    }

    private void printTable(List<KafkaRecord> messages) {
        List<MessageRow> rows = new ArrayList<>();

        for (KafkaRecord msg : messages) {
            rows.add(new MessageRow(
                    String.valueOf(msg.partition()),
                    String.valueOf(msg.offset()),
                    msg.keyString(""),
                    msg.valueString("")
            ));
        }

        String table = AsciiTable.getTable(AsciiTable.NO_BORDERS, rows, List.of(
                new Column().header("PARTITION").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.partition),
                new Column().header("OFFSET").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.RIGHT).with(r -> r.offset),
                new Column().header("KEY").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.key),
                new Column().header("VALUE").headerAlign(HorizontalAlign.LEFT).dataAlign(HorizontalAlign.LEFT).with(r -> r.value)
        ));

        out().println(table);
    }

    private void printTableRow(KafkaRecord msg) {
        out().printf("%d\t%d\t%s\t%s%n",
                msg.partition(),
                msg.offset(),
                msg.keyString(""),
                msg.valueString(""));
    }

    private void printJson(List<KafkaRecord> messages) {
        try {
            List<Map<String, Object>> messageList = new ArrayList<>();

            for (KafkaRecord msg : messages) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("partition", msg.partition());
                data.put("offset", msg.offset());
                data.put("key", msg.keyString(null));
                data.put("value", msg.valueString(null));
                data.put("timestamp", msg.timestamp());
                data.put("headers", convertHeadersToMapList(msg.headers()));
                messageList.add(data);
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
            out().println(jsonMapper.writeValueAsString(messageList));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private void printJsonMessage(KafkaRecord msg) {
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("partition", msg.partition());
            data.put("offset", msg.offset());
            data.put("key", msg.keyString(null));
            data.put("value", msg.valueString(null));
            data.put("timestamp", msg.timestamp());
            data.put("headers", convertHeadersToMapList(msg.headers()));

            ObjectMapper jsonMapper = new ObjectMapper();
            out().println(jsonMapper.writeValueAsString(data));
        } catch (Exception e) {
            err().println("Error: Failed to generate JSON output: " + e.getMessage());
        }
    }

    private void printYaml(List<KafkaRecord> messages) {
        try {
            List<Map<String, Object>> messageList = new ArrayList<>();

            for (KafkaRecord msg : messages) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("partition", msg.partition());
                data.put("offset", msg.offset());
                data.put("key", msg.keyString(null));
                data.put("value", msg.valueString(null));
                data.put("timestamp", msg.timestamp());
                data.put("headers", convertHeadersToMapList(msg.headers()));
                messageList.add(data);
            }

            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().println(yamlMapper.writeValueAsString(messageList));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    private void printYamlMessage(KafkaRecord msg) {
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("partition", msg.partition());
            data.put("offset", msg.offset());
            data.put("key", msg.keyString(null));
            data.put("value", msg.valueString(null));
            data.put("timestamp", msg.timestamp());
            data.put("headers", convertHeadersToMapList(msg.headers()));

            YAMLFactory yamlFactory = YAMLFactory.builder()
                    .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .build();
            ObjectMapper yamlMapper = new ObjectMapper(yamlFactory);
            out().println(yamlMapper.writeValueAsString(data));
        } catch (Exception e) {
            err().println("Error: Failed to generate YAML output: " + e.getMessage());
        }
    }

    /**
     * Convert headers to a list of maps for JSON/YAML serialization.
     * Each header becomes a map with "key" and "value" properties.
     */
    private List<Map<String, String>> convertHeadersToMapList(List<KafkaRecord.Header> headers) {
        List<Map<String, String>> headerList = new ArrayList<>();
        for (var header : headers) {
            Map<String, String> headerMap = new LinkedHashMap<>();
            headerMap.put("key", header.key());
            headerMap.put("value", header.valueString(null));
            headerList.add(headerMap);
        }
        return headerList;
    }

    private static record MessageRow(String partition, String offset, String key, String value) {}
}
