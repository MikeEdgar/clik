package io.streamshub.clik.command.topic;

import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "create",
        description = "Create a new Kafka topic"
)
public class CreateTopicCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String name;

    @CommandLine.Option(
            names = {"--partitions", "-p"},
            description = "Number of partitions (default: ${DEFAULT-VALUE})",
            defaultValue = "1"
    )
    int partitions;

    @CommandLine.Option(
            names = {"--replication-factor", "-r"},
            description = "Replication factor (default: ${DEFAULT-VALUE})",
            defaultValue = "1"
    )
    int replicationFactor;

    @CommandLine.Option(
            names = {"--config", "-c"},
            description = "Topic configuration (repeatable, format: key=value)"
    )
    Map<String, String> configs = new HashMap<>();

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        try (Admin admin = clientFactory.createAdminClient()) {
            topicService.createTopic(admin, name, partitions, replicationFactor, configs);
            System.out.println("Topic \"" + name + "\" created.");
            return 0;
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: Failed to create topic: " + e.getMessage());
            return 1;
        }
    }
}
