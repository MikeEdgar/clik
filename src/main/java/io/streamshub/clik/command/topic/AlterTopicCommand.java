package io.streamshub.clik.command.topic;

import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import io.streamshub.clik.kafka.model.TopicInfo;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "alter",
        description = "Alter topic configuration and partitions"
)
public class AlterTopicCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String name;

    @CommandLine.Option(
            names = {"-c", "--config"},
            description = "Set configuration (key=value, repeatable)"
    )
    List<String> configs = new ArrayList<>();

    @CommandLine.Option(
            names = {"--delete-config"},
            description = "Delete configuration (key, repeatable)"
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
            System.err.println("Error: At least one --config, --delete-config, or --partitions option must be specified.");
            return 1;
        }

        // Parse config key=value pairs
        Map<String, String> configMap = new HashMap<>();
        for (String config : configs) {
            String[] parts = config.split("=", 2);
            if (parts.length != 2) {
                System.err.println("Error: Invalid config format: " + config);
                System.err.println("Expected format: key=value");
                return 1;
            }
            configMap.put(parts[0].trim(), parts[1].trim());
        }

        // Validate partition count if specified
        int currentPartitions = 0;
        if (partitions != null) {
            try (Admin admin = clientFactory.createAdminClient()) {
                TopicInfo topicInfo = topicService.describeTopic(admin, name);
                currentPartitions = topicInfo.partitions();

                if (partitions <= currentPartitions) {
                    System.err.println("Error: New partition count (" + partitions +
                        ") must be greater than current count (" + currentPartitions + ").");
                    System.err.println("Kafka does not support decreasing partition count.");
                    return 1;
                }
            } catch (Exception e) {
                // Handle topic not found error
                Throwable cause = e.getCause();
                if (cause instanceof UnknownTopicOrPartitionException) {
                    System.err.println("Error: Topic \"" + name + "\" not found.");
                    System.err.println();
                    System.err.println("Run 'clik topic list' to see available topics.");
                    return 1;
                }
                System.err.println("Error: Failed to describe topic: " + e.getMessage());
                return 1;
            }
        }

        try (Admin admin = clientFactory.createAdminClient()) {
            boolean configsAltered = false;
            boolean partitionsAltered = false;

            // Alter configs if specified
            if (!configMap.isEmpty() || !deleteConfigs.isEmpty()) {
                topicService.alterTopicConfig(admin, name, configMap, deleteConfigs);
                configsAltered = true;
            }

            // Increase partitions if specified
            if (partitions != null) {
                topicService.increasePartitions(admin, name, partitions);
                partitionsAltered = true;
            }

            // Build success message
            if (configsAltered && partitionsAltered) {
                System.out.println("Topic \"" + name + "\" partitions increased from " +
                    currentPartitions + " to " + partitions + " and configuration altered.");
            } else if (partitionsAltered) {
                System.out.println("Topic \"" + name + "\" partitions increased from " +
                    currentPartitions + " to " + partitions + ".");
            } else {
                System.out.println("Topic \"" + name + "\" configuration altered.");
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                System.err.println("Error: Topic \"" + name + "\" not found.");
                System.err.println();
                System.err.println("Run 'clik topic list' to see available topics.");
                return 1;
            }
            System.err.println("Error: Failed to alter topic: " + e.getMessage());
            return 1;
        }
    }
}
