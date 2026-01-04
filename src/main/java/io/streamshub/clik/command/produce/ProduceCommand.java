package io.streamshub.clik.command.produce;

import io.streamshub.clik.kafka.KafkaClientFactory;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@CommandLine.Command(
        name = "produce",
        description = "Produce messages to a Kafka topic"
)
public class ProduceCommand implements Callable<Integer> {

    @CommandLine.Spec
    CommandSpec commandSpec;

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
        // Validate mutually exclusive options
        if (file != null && interactive) {
            err().println("Error: --file and --interactive options are mutually exclusive");
            return 1;
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

        try (Producer<String, String> producer = clientFactory.createProducer();
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
        if (file != null) {
            return Files.lines(Paths.get(file));
        } else if (interactive) {
            err().println("Enter messages (Ctrl+D to finish):");
            return new BufferedReader(new InputStreamReader(System.in)).lines();
        } else {
            return new BufferedReader(new InputStreamReader(System.in)).lines();
        }
    }

    private int sendMessages(Producer<String, String> producer, Stream<String> messages) {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        messages.forEach(message -> {
            ProducerRecord<String, String> rec = new ProducerRecord<>(
                    topic, partition, key, message);

            producer.send(rec, (metadata, exception) -> {
                if (exception == null) {
                    successCount.incrementAndGet();
                } else {
                    failureCount.incrementAndGet();
                    err().println("Failed to send message: " + exception.getMessage());
                }
            });
        });

        producer.flush();

        out().println(successCount.get() + " messages sent successfully");
        if (failureCount.get() > 0) {
            err().println(failureCount.get() + " messages failed");
        }

        return failureCount.get() > 0 ? 1 : 0;
    }
}
