package io.streamshub.clik.command.group;

import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(
        name = "alter",
        description = "Alter consumer group offsets"
)
public class AlterGroupCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Group ID"
    )
    String groupId;

    @CommandLine.Option(
            names = {"--to-earliest"},
            description = "Reset to earliest offset (optionally specify topic[:partition])"
    )
    List<String> toEarliest = new ArrayList<>();

    @CommandLine.Option(
            names = {"--to-latest"},
            description = "Reset to latest offset (optionally specify topic[:partition])"
    )
    List<String> toLatest = new ArrayList<>();

    @CommandLine.Option(
            names = {"--to-offset"},
            description = "Set to specific offset (format: offset:topic[:partition])"
    )
    List<String> toOffset = new ArrayList<>();

    @CommandLine.Option(
            names = {"--shift-by"},
            description = "Shift offset by N (format: offset:topic[:partition])"
    )
    List<String> shiftBy = new ArrayList<>();

    @CommandLine.Option(
            names = {"--to-datetime"},
            description = "Reset to datetime in ISO-8601 format (optionally specify topic[:partition], e.g., '2026-01-01T00:00:00Z:mytopic:0')"
    )
    List<String> toDatetime = new ArrayList<>();

    @CommandLine.Option(
            names = {"--by-duration"},
            description = "Shift back by duration in ISO-8601 format (e.g., 'PT1H:mytopic' for 1 hour)"
    )
    List<String> byDuration = new ArrayList<>();

    @CommandLine.Option(
            names = {"--delete"},
            description = "Delete offsets (optionally specify topic[:partition])"
    )
    List<String> delete = new ArrayList<>();

    @CommandLine.Option(
            names = {"-y", "--yes"},
            description = "Automatically confirm alteration without prompting"
    )
    boolean autoConfirm;

    @Inject
    KafkaClientFactory clientFactory;

    @Inject
    GroupService groupService;

    @Override
    public Integer call() {
        // Validate at least one option is specified
        if (toEarliest.isEmpty() && toLatest.isEmpty() && toOffset.isEmpty() &&
                shiftBy.isEmpty() && toDatetime.isEmpty() && byDuration.isEmpty() && delete.isEmpty()) {
            System.err.println("Error: At least one offset option must be specified.");
            System.err.println();
            System.err.println("Available options:");
            System.err.println("  --to-earliest [topic[:partition]]    Reset to earliest offset");
            System.err.println("  --to-latest [topic[:partition]]      Reset to latest offset");
            System.err.println("  --to-offset offset:topic[:partition] Set to specific offset");
            System.err.println("  --shift-by offset:topic[:partition]  Shift offset by N");
            System.err.println("  --to-datetime datetime[:topic[:partition]]  Reset to timestamp");
            System.err.println("  --by-duration duration[:topic[:partition]]  Shift by duration");
            System.err.println("  --delete [topic[:partition]]         Delete offsets");
            return 1;
        }

        try (Admin admin = clientFactory.createAdminClient()) {
            // Check if group exists and has no active members
            boolean hasMembers = groupService.hasActiveMembers(admin, groupId);
            if (hasMembers) {
                System.err.println("Error: Group \"" + groupId + "\" has active members.");
                System.err.println("Stop all consumers before altering offsets.");
                return 1;
            }

            // Get current group offsets
            Map<TopicPartition, OffsetAndMetadata> currentOffsets;
            try {
                currentOffsets = groupService.getGroupOffsetMap(admin, groupId);
            } catch (Exception e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                    System.err.println("Error: Group \"" + groupId + "\" not found.");
                    System.err.println();
                    System.err.println("Run 'clik group list' to see available groups.");
                    return 1;
                }
                System.err.println("Error: Failed to get group offsets: " + e.getMessage());
                return 1;
            }

            Set<TopicPartition> groupPartitions = currentOffsets.keySet();

            if (groupPartitions.isEmpty()) {
                System.err.println("Error: Group \"" + groupId + "\" has no committed offsets.");
                return 1;
            }

            // Prompt for confirmation unless --yes
            if (!autoConfirm) {
                System.out.print("Alter offsets for group \"" + groupId + "\"? This cannot be undone. [y/N]: ");
                String response;
                try (Scanner scanner = new Scanner(System.in)) {
                    response = scanner.nextLine().trim().toLowerCase();
                }

                if (!response.equals("y") && !response.equals("yes")) {
                    System.out.println("Alter cancelled.");
                    return 0;
                }
            }

            // Process each offset operation
            Map<TopicPartition, OffsetAndMetadata> offsetsToAlter = new HashMap<>();
            Set<TopicPartition> offsetsToDelete = new HashSet<>();

            // Process --to-earliest
            for (String spec : toEarliest) {
                Set<TopicPartition> partitions = parseTopicPartitionSpec(spec, groupPartitions);
                Map<TopicPartition, Long> earliest = getEarliestOffsets(admin, partitions);
                for (Map.Entry<TopicPartition, Long> entry : earliest.entrySet()) {
                    offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                }
            }

            // Process --to-latest
            for (String spec : toLatest) {
                Set<TopicPartition> partitions = parseTopicPartitionSpec(spec, groupPartitions);
                Map<TopicPartition, Long> latest = getLatestOffsets(admin, partitions);
                for (Map.Entry<TopicPartition, Long> entry : latest.entrySet()) {
                    offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                }
            }

            // Process --to-offset
            for (String spec : toOffset) {
                Map.Entry<Long, Set<TopicPartition>> parsed = parseOffsetSpec(spec, groupPartitions);
                long offset = parsed.getKey();
                for (TopicPartition tp : parsed.getValue()) {
                    offsetsToAlter.put(tp, new OffsetAndMetadata(offset));
                }
            }

            // Process --shift-by
            for (String spec : shiftBy) {
                Map.Entry<Long, Set<TopicPartition>> parsed = parseOffsetSpec(spec, groupPartitions);
                long shift = parsed.getKey();
                for (TopicPartition tp : parsed.getValue()) {
                    long currentOffset = currentOffsets.get(tp).offset();
                    long newOffset = currentOffset + shift;
                    if (newOffset < 0) {
                        System.err.println("Error: Shift-by would result in negative offset for " + tp);
                        return 1;
                    }
                    offsetsToAlter.put(tp, new OffsetAndMetadata(newOffset));
                }
            }

            // Process --to-datetime
            for (String spec : toDatetime) {
                Map.Entry<String, Set<TopicPartition>> parsed = parseDatetimeSpec(spec, groupPartitions);
                String datetime = parsed.getKey();
                try {
                    Instant instant = Instant.parse(datetime);
                    long timestamp = instant.toEpochMilli();
                    Map<TopicPartition, Long> timestampOffsets = getOffsetsForTimestamp(admin, parsed.getValue(), timestamp);
                    for (Map.Entry<TopicPartition, Long> entry : timestampOffsets.entrySet()) {
                        offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                    }
                } catch (DateTimeParseException e) {
                    System.err.println("Error: Invalid ISO-8601 datetime format: " + datetime);
                    System.err.println("Expected format: 2026-01-01T00:00:00Z");
                    return 1;
                }
            }

            // Process --by-duration
            for (String spec : byDuration) {
                Map.Entry<String, Set<TopicPartition>> parsed = parseDatetimeSpec(spec, groupPartitions);
                String durationStr = parsed.getKey();
                try {
                    Duration duration = Duration.parse(durationStr);
                    long timestamp = Instant.now().minus(duration).toEpochMilli();
                    Map<TopicPartition, Long> timestampOffsets = getOffsetsForTimestamp(admin, parsed.getValue(), timestamp);
                    for (Map.Entry<TopicPartition, Long> entry : timestampOffsets.entrySet()) {
                        offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                    }
                } catch (DateTimeParseException e) {
                    System.err.println("Error: Invalid ISO-8601 duration format: " + durationStr);
                    System.err.println("Expected format: PT1H (1 hour), PT30M (30 minutes), etc.");
                    return 1;
                }
            }

            // Process --delete
            for (String spec : delete) {
                Set<TopicPartition> partitions = parseTopicPartitionSpec(spec, groupPartitions);
                offsetsToDelete.addAll(partitions);
            }

            // Execute operations
            int alteredCount = 0;
            int deletedCount = 0;

            if (!offsetsToAlter.isEmpty()) {
                groupService.alterGroupOffsets(admin, groupId, offsetsToAlter);
                alteredCount = offsetsToAlter.size();
            }

            if (!offsetsToDelete.isEmpty()) {
                groupService.deleteGroupOffsets(admin, groupId, offsetsToDelete);
                deletedCount = offsetsToDelete.size();
            }

            // Print success message
            if (alteredCount > 0 && deletedCount > 0) {
                System.out.println("Altered offsets for " + alteredCount + " partition(s) and deleted offsets for " +
                        deletedCount + " partition(s) in group \"" + groupId + "\".");
            } else if (alteredCount > 0) {
                System.out.println("Altered offsets for " + alteredCount + " partition(s) in group \"" + groupId + "\".");
            } else if (deletedCount > 0) {
                System.out.println("Deleted offsets for " + deletedCount + " partition(s) from group \"" + groupId + "\".");
            }

            return 0;
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("Error: Failed to alter group offsets: " + e.getMessage());
            return 1;
        }
    }

    /**
     * Parse topic:partition specification
     * Supports: "", "topic", "topic:partition"
     */
    private Set<TopicPartition> parseTopicPartitionSpec(String spec, Set<TopicPartition> groupPartitions) {
        if (spec == null || spec.isEmpty()) {
            // All partitions in group
            return groupPartitions;
        }

        String[] parts = spec.split(":", 2);
        String topic = parts[0];

        if (parts.length == 1) {
            // All partitions for this topic
            return groupPartitions.stream()
                    .filter(tp -> tp.topic().equals(topic))
                    .collect(Collectors.toSet());
        }

        // Specific partition
        try {
            int partition = Integer.parseInt(parts[1]);
            TopicPartition tp = new TopicPartition(topic, partition);

            // Verify partition exists in group
            if (!groupPartitions.contains(tp)) {
                throw new IllegalArgumentException(
                        "Partition " + tp + " not found in group offsets");
            }

            return Set.of(tp);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid partition number: " + parts[1]);
        }
    }

    /**
     * Parse offset specification (offset:topic[:partition])
     */
    private Map.Entry<Long, Set<TopicPartition>> parseOffsetSpec(String spec, Set<TopicPartition> groupPartitions) {
        String[] parts = spec.split(":", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "Invalid format: expected 'offset:topic[:partition]', got: " + spec);
        }

        try {
            long offset = Long.parseLong(parts[0]);
            if (offset < 0) {
                throw new IllegalArgumentException("Offset must be non-negative: " + offset);
            }

            Set<TopicPartition> partitions = parseTopicPartitionSpec(parts[1], groupPartitions);
            return Map.entry(offset, partitions);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid offset number: " + parts[0]);
        }
    }

    /**
     * Parse datetime/duration specification (datetime[:topic[:partition]])
     */
    private Map.Entry<String, Set<TopicPartition>> parseDatetimeSpec(String spec, Set<TopicPartition> groupPartitions) {
        // Find the topic:partition part by looking for the last occurrence that matches a topic name
        // This handles ISO-8601 timestamps which contain colons
        String datetime = spec;
        String topicPartSpec = "";

        // Try to find topic:partition suffix
        for (TopicPartition tp : groupPartitions) {
            String topic = tp.topic();
            int topicIndex = spec.lastIndexOf(":" + topic);
            if (topicIndex > 0) {
                datetime = spec.substring(0, topicIndex);
                topicPartSpec = spec.substring(topicIndex + 1);
                break;
            }
        }

        Set<TopicPartition> partitions = parseTopicPartitionSpec(topicPartSpec, groupPartitions);
        return Map.entry(datetime, partitions);
    }

    private Map<TopicPartition, Long> getEarliestOffsets(Admin admin, Set<TopicPartition> partitions) throws Exception {
        Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
        for (TopicPartition tp : partitions) {
            offsetSpecs.put(tp, OffsetSpec.earliest());
        }

        ListOffsetsResult result = admin.listOffsets(offsetSpecs);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = result.all().get();

        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
            offsetMap.put(entry.getKey(), entry.getValue().offset());
        }
        return offsetMap;
    }

    private Map<TopicPartition, Long> getLatestOffsets(Admin admin, Set<TopicPartition> partitions) throws Exception {
        Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
        for (TopicPartition tp : partitions) {
            offsetSpecs.put(tp, OffsetSpec.latest());
        }

        ListOffsetsResult result = admin.listOffsets(offsetSpecs);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = result.all().get();

        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
            offsetMap.put(entry.getKey(), entry.getValue().offset());
        }
        return offsetMap;
    }

    private Map<TopicPartition, Long> getOffsetsForTimestamp(Admin admin, Set<TopicPartition> partitions, long timestamp) throws Exception {
        Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
        for (TopicPartition tp : partitions) {
            offsetSpecs.put(tp, OffsetSpec.forTimestamp(timestamp));
        }

        ListOffsetsResult result = admin.listOffsets(offsetSpecs);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = result.all().get();

        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
            offsetMap.put(entry.getKey(), entry.getValue().offset());
        }
        return offsetMap;
    }
}
