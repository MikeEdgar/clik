package io.streamshub.clik.command.topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.ConfigCandidates;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.kafka.model.TopicInfo;
import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;

@CommandLine.Command(
        name = "alter",
        description = "Alter topic configuration and partitions"
)
public class AlterTopicCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name",
            completionCandidates = NameCandidate.Topic.class
    )
    String name;

    @CommandLine.Option(
            names = {"--config", "-c"},
            description = "Topic configuration (repeatable, format: key=value)",
            paramLabel = "config",
            completionCandidates = ConfigCandidates.Topic.class
    )
    Map<String, String> configs = new HashMap<>();

    @CommandLine.Option(
            names = {"--delete-config"},
            description = "Delete configuration (key, repeatable)",
            paramLabel = "delete-config",
            completionCandidates = ConfigCandidates.Topic.class
    )
    List<String> deleteConfigs = new ArrayList<>();

    @CommandLine.Option(
            names = {"--partitions"},
            description = "New partition count (can only increase)"
    )
    Integer partitions;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        if (configs.isEmpty() && deleteConfigs.isEmpty() && partitions == null) {
            err().println("Error: At least one --config, --delete-config, or --partitions option must be specified.");
            return 1;
        }

        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            AtomicInteger currentPartitions = new AtomicInteger(0);

            if (!validatePartitions(admin, currentPartitions)) {
                return 1;
            }

            boolean configsAltered = false;
            boolean partitionsAltered = false;

            // Alter configs if specified
            if (!configs.isEmpty() || !deleteConfigs.isEmpty()) {
                topicService.alterTopicConfig(admin, name, configs, deleteConfigs);
                configsAltered = true;
            }

            // Increase partitions if specified
            if (partitions != null) {
                topicService.increasePartitions(admin, name, partitions);
                partitionsAltered = true;
            }

            printResults(configsAltered, partitionsAltered, currentPartitions.get());
            return 0;
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                err().printf("Error: Topic \"%s\" not found.%n", name);
                err().println();
                err().println("Run 'clik topic list' to see available topics.");
                return 1;
            }
            err().println("Error: Failed to alter topic: " + e.getMessage());
            return 1;
        }
    }

    private boolean validatePartitions(Admin admin, AtomicInteger currentPartitions) {
        if (partitions != null) {
            TopicInfo topicInfo = topicService.describeTopic(admin, name);
            int currPartitions = topicInfo.partitions();

            if (partitions <= currPartitions) {
                err().printf("Error: New partition count (%d) must be greater than current count (%d).%n", 
                        partitions,
                        currPartitions);
                err().println("Kafka does not support decreasing partition count.");
                return false;
            }

            currentPartitions.set(currPartitions);
        }

        return true;
    }

    /**
     * Build success message
     */
    private void printResults(boolean configsAltered, boolean partitionsAltered, int currentPartitions) {
        // Build success message
        if (configsAltered && partitionsAltered) {
            out().printf("Topic \"%s\" partitions increased from %d to %d and configuration altered.%n",
                    name, currentPartitions, partitions);
        } else if (partitionsAltered) {
            out().printf("Topic \"%s\" partitions increased from %d to %d.%n",
                    name,
                    currentPartitions,
                    partitions);
        } else {
            out().printf("Topic \"%s\" configuration altered.%n", name);
        }
    }
}
