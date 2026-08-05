package io.streamshub.clik.command.topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.ConfigCandidates;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.kafka.TopicService;
import picocli.CommandLine;

@CommandLine.Command(
        name = "create",
        description = "Create a new Kafka topic"
)
public class CreateTopicCommand extends ContextualCommand implements Callable<Integer> {

    static class ReplicationSpec {
        @CommandLine.Option(
                names = {"--replication-factor", "-r"},
                description = "Replication factor"
        )
        Optional<Integer> replicationFactor = Optional.empty();

        @CommandLine.Option(
                names = {"--replica-assignment"},
                description = "Manual replica assignment as a comma-separated list of colon-separated broker IDs, one entry per partition (e.g. 101:102,102:103,103:101)",
                split = ","
        )
        List<String> replicaAssignment;
    }

    @CommandLine.Parameters(
            index = "0",
            description = "Topic name"
    )
    String name;

    @CommandLine.Option(
            names = {"--partitions", "-p"},
            description = "Number of partitions"
    )
    Optional<Integer> partitions = Optional.empty();

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
    ReplicationSpec replicationSpec;

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
        var replicaAssignment = replicaAssignments();

        if (replicaAssignment.isPresent() && partitions.isPresent()) {
            err().println("Error: --partitions cannot be used with --replica-assignment");
            return 1;
        }

        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            if (replicaAssignment.isPresent()) {
                Map<Integer, List<Integer>> replicaAssignmentMap = parseReplicaAssignments(replicaAssignment.get());
                topicService.createTopic(admin, name, replicaAssignmentMap, configs);
            } else {
                int effectivePartitions = partitions.orElse(1);
                int effectiveReplicationFactor = replicationFactor().orElse(1);
                topicService.createTopic(admin, name, effectivePartitions, effectiveReplicationFactor, configs);
            }
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

    private Optional<List<String>> replicaAssignments() {
        return Optional.ofNullable(replicationSpec).map(r -> r.replicaAssignment);
    }

    private Optional<Integer> replicationFactor() {
        return Optional.ofNullable(replicationSpec).flatMap(r -> r.replicationFactor);
    }

    private Map<Integer, List<Integer>> parseReplicaAssignments(List<String> assignments) {
        Map<Integer, List<Integer>> result = new LinkedHashMap<>();
        for (int partition = 0; partition < assignments.size(); partition++) {
            String entry = assignments.get(partition);
            String[] parts = entry.split(":");
            List<Integer> brokers = new ArrayList<>(parts.length);
            for (String part : parts) {
                try {
                    brokers.add(Integer.parseInt(part.strip()));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid --replica-assignment format: \"" + part.strip() + "\" is not a valid broker ID");
                }
            }
            result.put(partition, brokers);
        }
        return result;
    }
}
