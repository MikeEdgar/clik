package io.streamshub.clik.command.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import io.streamshub.clik.command.BaseCommand;
import io.streamshub.clik.kafka.ConfigCandidates;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import picocli.CommandLine;

@CommandLine.Command(
        name = "create",
        description = "Create a new Kafka topic"
)
public class CreateTopicCommand extends BaseCommand implements Callable<Integer> {

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
            description = "Topic configuration (repeatable, format: key=value)",
            paramLabel = "config",
            completionCandidates = ConfigCandidates.Topic.class
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
            out().println("Topic \"" + name + "\" created.");
            return 0;
        } catch (IllegalArgumentException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to create topic: " + e.getMessage());
            return 1;
        }
    }
}
