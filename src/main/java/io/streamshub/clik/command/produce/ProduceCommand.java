package io.streamshub.clik.command.produce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.ConfigCandidates;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.model.KafkaRecord;
import io.streamshub.clik.support.Encoding;
import io.streamshub.clik.support.InputParser;
import picocli.CommandLine;

@CommandLine.Command(
        name = "produce",
        description = "Produce messages to a Kafka topic"
)
public class ProduceCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String topic;

    @CommandLine.Option(
            names = {"-f", "--file"},
            description = "Read messages from file (one message per line)"
    )
    String file;

    @CommandLine.Option(
            names = {"-i", "--interactive"},
            description = "Interactive mode (prompt for messages)"
    )
    boolean interactive;

    @CommandLine.Option(
            names = {"-k", "--key"},
            description = "Message key (applied to all messages)"
    )
    String key;

    @CommandLine.Option(
            names = {"-p", "--partition"},
            description = "Target partition"
    )
    Integer partition;

    @CommandLine.Option(
            names = {"-v", "--value"},
            description = "Message value (mutually exclusive with --file and --interactive)"
    )
    String value;

    @CommandLine.Option(
            names = {"--header"},
            description = "Message header in format key=value (repeatable, supports duplicate keys)"
    )
    List<String> headers;

    @CommandLine.Option(
            names = {"-t", "--timestamp"},
            description = "Message timestamp (epoch milliseconds or ISO-8601 format, e.g., 2026-01-04T12:00:00Z)"
    )
    String timestamp;

    @CommandLine.Option(
            names = {"-I", "--input"},
            description = "Format string for parsing input lines (e.g., '%k %v' for key-value pairs)"
    )
    String inputFormat;

    @CommandLine.Option(
            names = {"--property", "-P"},
            description = "Producer client properties to override those in the context (repeatable, format: key=value)",
            paramLabel = "property",
            completionCandidates = ConfigCandidates.Producer.class
    )
    Map<String, String> properties = new HashMap<>();

    @Inject
    KafkaClientFactory clientFactory;

    @Override
    public Integer call() {
        // Validate mutually exclusive options
        int inputModeCount = 0;

        if (file != null) {
            inputModeCount++;
        }
        if (interactive) {
            inputModeCount++;
        }
        if (value != null) {
            inputModeCount++;
        }

        if (inputModeCount > 1) {
            err().println("Error: --file, --interactive, and --value are mutually exclusive");
            return 1;
        }

        // --input cannot be used with --value
        if (inputFormat != null && value != null) {
            err().println("Error: --input cannot be used with --value");
            return 1;
        }

        // --input cannot be used with global --key, --header, --timestamp, or --partition
        if (inputFormat != null) {
            if (key != null) {
                err().println("Error: --input cannot be used with --key (use %k in format string instead)");
                return 1;
            }
            if (headers != null && !headers.isEmpty()) {
                err().println("Error: --input cannot be used with --header (use %h in format string instead)");
                return 1;
            }
            if (timestamp != null) {
                err().println("Error: --input cannot be used with --timestamp (use %T in format string instead)");
                return 1;
            }
            if (partition != null) {
                err().println("Error: --input cannot be used with --partition (use %p in format string instead)");
                return 1;
            }
        }

        // Validate file exists if specified
        if (file != null) {
            Path filePath = Paths.get(file);
            if (!Files.exists(filePath)) {
                err().println("Error: File not found: " + file);
                return 1;
            }
            if (!Files.isRegularFile(filePath)) {
                err().println("Error: Not a regular file: " + file);
                return 1;
            }
        }

        try (Producer<byte[], byte[]> producer = clientFactory.createProducer(contextName, properties);
            Stream<String> messages = readMessages()) {
            return sendMessages(producer, messages);
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (IOException e) {
            err().println("Error: Failed to read messages: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to produce messages: " + e.getMessage());
            return 1;
        }
    }

    private Stream<String> readMessages() throws IOException {
        if (value != null) {
            return Stream.of(value);
        } else if (file != null) {
            return Files.lines(Paths.get(file));
        } else if (interactive) {
            err().println("Enter messages (Ctrl+D to finish):");
            return new BufferedReader(new InputStreamReader(System.in)).lines();
        } else {
            return new BufferedReader(new InputStreamReader(System.in)).lines();
        }
    }

    private Headers parseHeaders(List<String> headerList) {
        if (headerList == null || headerList.isEmpty()) {
            return null;
        }

        RecordHeaders recordHeaders = new RecordHeaders();

        for (String header : headerList) {
            int equalsIndex = header.indexOf('=');
            if (equalsIndex <= 0) {
                throw new IllegalArgumentException(
                        "Invalid header format: " + header +
                                ". Expected format: key=value");
            }

            String headerKey = header.substring(0, equalsIndex).trim();
            String headerValue = header.substring(equalsIndex + 1).trim();

            // Use EncodingUtil to support base64: and hex: prefixes
            byte[] headerValueBytes = Encoding.decodeValue(headerValue);
            recordHeaders.add(headerKey, headerValueBytes);
        }

        return recordHeaders;
    }

    private Headers mapHeaders(List<KafkaRecord.Header> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }

        RecordHeaders recordHeaders = new RecordHeaders();
        for (KafkaRecord.Header entry : headers) {
            recordHeaders.add(entry.key(), entry.valueBytes());
        }
        return recordHeaders;
    }

    private Long parseTimestamp(String ts) {
        if (ts == null || ts.isEmpty()) {
            return null;
        }

        // Try parsing as epoch milliseconds first
        try {
            return Long.parseLong(ts);
        } catch (NumberFormatException _) {
            // Not a number, try parsing as ISO-8601
            try {
                return OffsetDateTime.parse(ts).toInstant().toEpochMilli();
            } catch (DateTimeParseException _) {
                throw new IllegalArgumentException(
                        "Invalid timestamp format: " + ts +
                                ". Expected epoch milliseconds or ISO-8601 format (e.g., 2026-01-04T12:00:00Z)");
            }
        }
    }

    private int sendMessages(Producer<byte[], byte[]> producer, Stream<String> messages) {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        if (inputFormat != null) {
            // Parse format string once
            InputParser format;
            try {
                format = InputParser.withFormat(inputFormat);
            } catch (IllegalArgumentException e) {
                err().println("Error: Invalid format string: " + e.getMessage());
                return 1;
            }

            // Process each line according to format string
            messages.forEach(line -> {
                try {
                    // Parse line according to format
                    KafkaRecord components = format.parse(line);

                    // Create producer record from parsed components
                    ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(
                            topic,
                            components.partition(),
                            components.timestamp(),
                            components.keyBytes(),
                            components.valueBytes(),
                            mapHeaders(components.headers()));

                    producer.send(rec, (_, exception) -> {
                        if (exception == null) {
                            successCount.incrementAndGet();
                        } else {
                            failureCount.incrementAndGet();
                            err().println("Failed to send message: " + exception.getMessage());
                        }
                    });
                } catch (IllegalArgumentException e) {
                    failureCount.incrementAndGet();
                    err().println("Failed to parse line: " + e.getMessage());
                }
            });
        } else {
            // Existing behavior: use global options
            Headers recordHeaders = parseHeaders(headers);
            Long timestampMillis = parseTimestamp(timestamp);
            byte[] keyBytes = key != null ? Encoding.decodeValue(key) : null;

            messages.forEach(message -> {
                // Decode message value (may have base64: or hex: prefix)
                byte[] valueBytes = Encoding.decodeValue(message);

                // Use headers and/or timestamp may be null
                ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(
                        topic,
                        partition,
                        timestampMillis,
                        keyBytes,
                        valueBytes,
                        recordHeaders);

                producer.send(rec, (_, exception) -> {
                    if (exception == null) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                        err().println("Failed to send message: " + exception.getMessage());
                    }
                });
            });
        }

        producer.flush();

        out().println(successCount.get() + " messages sent successfully");
        if (failureCount.get() > 0) {
            err().println(failureCount.get() + " messages failed");
        }

        return failureCount.get() > 0 ? 1 : 0;
    }
}
