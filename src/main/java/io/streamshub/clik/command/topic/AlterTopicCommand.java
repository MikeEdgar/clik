package io.streamshub.clik.command.topic;

import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
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
        description = "Alter topic configuration"
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

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    TopicService topicService;

    @Override
    public Integer call() {
        if (configs.isEmpty() && deleteConfigs.isEmpty()) {
            System.err.println("Error: At least one --config or --delete-config option must be specified.");
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

        try (Admin admin = clientFactory.createAdminClient()) {
            topicService.alterTopicConfig(admin, name, configMap, deleteConfigs);
            System.out.println("Topic \"" + name + "\" configuration altered.");
            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            // Check if it's an unknown topic exception
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                System.err.println("Error: Topic \"" + name + "\" not found.");
                System.err.println();
                System.err.println("Run 'clik topic list' to see available topics.");
                return 1;
            }
            System.err.println("Error: Failed to alter topic configuration: " + e.getMessage());
            return 1;
        }
    }
}
