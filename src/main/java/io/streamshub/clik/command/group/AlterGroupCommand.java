package io.streamshub.clik.command.group;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.streamshub.clik.command.ContextualCommand;
import io.streamshub.clik.kafka.GroupService;
import io.streamshub.clik.kafka.KafkaClientFactory;
import io.streamshub.clik.support.NameCandidate;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;

@CommandLine.Command(
        name = "alter",
        description = "Alter consumer group offsets"
)
public class AlterGroupCommand extends ContextualCommand implements Callable<Integer> {

    @CommandLine.Parameters(
            index = "0",
            description = "Group ID",
            completionCandidates = NameCandidate.Group.class
    )
    String groupId;

    @CommandLine.Option(
            names = {"--to-earliest"},
            description = "Reset to earliest offset (optionally specify topic[:partition])",
            preprocessor = AllTopicsOptionProcessor.class
    )
    List<String> toEarliest = new ArrayList<>();

    @CommandLine.Option(
            names = {"--to-latest"},
            description = "Reset to latest offset (optionally specify topic[:partition])",
            preprocessor = AllTopicsOptionProcessor.class
    )
    List<String> toLatest = new ArrayList<>();

    @CommandLine.Option(
            names = {"--to-offset"},
            description = "Set to specific offset (format: offset=topic:partition)"
    )
    List<String> toOffset = new ArrayList<>();

    @CommandLine.Option(
            names = {"--shift-by"},
            description = "Shift offset by N (format: offset=topic:partition)"
    )
    List<String> shiftBy = new ArrayList<>();

    @CommandLine.Option(
            names = {"--to-datetime"},
            description = "Reset to datetime in ISO-8601 format (optionally specify topic[:partition], e.g., '2026-01-01T00:00:00Z=mytopic:0')"
    )
    List<String> toDatetime = new ArrayList<>();

    @CommandLine.Option(
            names = {"--by-duration"},
            description = "Shift by duration in ISO-8601 format (e.g., 'PT1H=mytopic' for 1 hour forward, 'PT-1H=mytopic' for 1 hour back)"
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

    static class AllTopicsOptionProcessor implements CommandLine.IParameterPreprocessor {
        @Override
        public boolean preprocess(Stack<String> args,
                CommandSpec commandSpec,
                ArgSpec argSpec,
                Map<String, Object> info) {
            /*
             * When no parameter has been attached to the option (e.g. via '='), check if the
             * next argument is a an optional topic name or some other known option.
             *
             */
            if (noParameterPresent(info) && nextArgIsOption(args, commandSpec)) {
                args.push(""); // act as if the user specified an empty topic name
            }
            return false; // picocli's internal parsing is resumed for this option
        }

        private boolean noParameterPresent(Map<String, Object> info) {
            return " ".equals(info.get("separator"));
        }

        private boolean nextArgIsOption(Stack<String> args, CommandSpec commandSpec) {
            if (args.isEmpty()) {
                return true;
            }

            return commandSpec.args().stream()
                    .filter(OptionSpec.class::isInstance)
                    .map(OptionSpec.class::cast)
                    .map(OptionSpec::names)
                    .flatMap(Arrays::stream)
                    .anyMatch(args.peek()::equals);
        }
    }

    @Override
    public Integer call() {
        // Validate at least one option is specified
        if (offsetOptionsEmpty()) {
            err().println("Error: At least one offset option must be specified.");
            err().println();
            err().println("Available options:");
            err().println("  --to-earliest [topic[:partition]]    Reset to earliest offset");
            err().println("  --to-latest [topic[:partition]]      Reset to latest offset");
            err().println("  --to-offset offset=topic:partition   Set to specific offset");
            err().println("  --shift-by offset=topic:partition    Shift offset by N");
            err().println("  --to-datetime datetime[=topic[:partition]]  Reset to timestamp");
            err().println("  --by-duration duration[=topic[:partition]]  Shift by duration");
            err().println("  --delete [topic[:partition]]         Delete offsets");
            return 1;
        }

        try (Admin admin = clientFactory.createAdminClient(contextName)) {
            return process(admin);
        } catch (IllegalStateException e) {
            err().println("Error: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            err().println("Error: Failed to alter group offsets: " + e.getMessage());
            return 1;
        }
    }

    private boolean offsetOptionsEmpty() {
        return Stream.of(toEarliest, toLatest, toOffset, shiftBy, toDatetime, byDuration, delete)
                .allMatch(Collection::isEmpty);
    }

    private int process(Admin admin) throws Exception {
        // Check if group exists and has no active members
        if (!validGroup(admin)) {
            return 1;
        }

        // Get current group offsets
        Map<TopicPartition, OffsetAndMetadata> currentOffsets;
        try {
            currentOffsets = groupService.getGroupOffsetMap(admin, groupId);
        } catch (Exception e) {
            err().println("Error: Failed to get group offsets: " + e.getMessage());
            return 1;
        }

        Set<TopicPartition> groupPartitions = currentOffsets.keySet();

        if (groupPartitions.isEmpty()) {
            err().println("Error: Group \"" + groupId + "\" has no committed offsets.");
            return 1;
        }

        // Prompt for confirmation unless --yes
        if (!autoConfirm) {
            out().print("Alter offsets for group \"" + groupId + "\"? This cannot be undone. [y/N]: ");
            String response;
            try (Scanner scanner = new Scanner(System.in)) {
                response = scanner.nextLine().trim().toLowerCase();
            }

            if (!response.equals("y") && !response.equals("yes")) {
                out().println("Alter cancelled.");
                return 0;
            }
        }

        // Process each offset operation
        Map<TopicPartition, OffsetAndMetadata> offsetsToAlter = new HashMap<>();
        Set<TopicPartition> offsetsToDelete = new HashSet<>();

        processToEarliest(admin, offsetsToAlter, groupPartitions);
        processToLatest(admin, offsetsToAlter, groupPartitions);
        processToOffset(offsetsToAlter, groupPartitions);

        if (processShiftBy(offsetsToAlter, groupPartitions, currentOffsets)) {
            return 1;
        }

        if (processToDatetime(admin, offsetsToAlter, groupPartitions)) {
            return 1;
        }

        if (processByDuration(admin, offsetsToAlter, groupPartitions, currentOffsets)) {
            return 1;
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

        printResults(groupId, alteredCount, deletedCount);

        return 0;
    }

    /**
     * Check if group exists and has no active members
     */
    private boolean validGroup(Admin admin) {
        boolean hasMembers;

        try {
            hasMembers = groupService.hasActiveMembers(admin, groupId);
        } catch (Exception e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                err().println("Error: Group \"" + groupId + "\" not found.");
                err().println();
                err().println("Run 'clik group list' to see available groups.");
                return false;
            }
            err().println("Error: Failed to retrieve group status: " + e.getMessage());
            return false;
        }

        if (hasMembers) {
            err().println("Error: Group \"" + groupId + "\" has active members.");
            err().println("Stop all consumers before altering offsets.");
            return false;
        }

        return true;
    }

    /**
     * Process --to-earliest
     */
    private void processToEarliest(Admin admin, Map<TopicPartition, OffsetAndMetadata> offsetsToAlter, Set<TopicPartition> groupPartitions) {
        for (String spec : toEarliest) {
            Set<TopicPartition> partitions = parseTopicPartitionSpec(spec, groupPartitions);
            Map<TopicPartition, Long> earliest = getEarliestOffsets(admin, partitions);
            for (Map.Entry<TopicPartition, Long> entry : earliest.entrySet()) {
                offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }
        }
    }

    /**
     * Process --to-latest
     */
    private void processToLatest(Admin admin, Map<TopicPartition, OffsetAndMetadata> offsetsToAlter, Set<TopicPartition> groupPartitions) {
        for (String spec : toLatest) {
            Set<TopicPartition> partitions = parseTopicPartitionSpec(spec, groupPartitions);
            Map<TopicPartition, Long> latest = getLatestOffsets(admin, partitions);
            for (Map.Entry<TopicPartition, Long> entry : latest.entrySet()) {
                offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            }
        }
    }

    /**
     * Process --to-offset
     */
    private void processToOffset(Map<TopicPartition, OffsetAndMetadata> offsetsToAlter, Set<TopicPartition> groupPartitions) {
        for (String spec : toOffset) {
            var parsed = parseOffsetSpec(spec, groupPartitions, false);
            long offset = parsed.value();
            for (TopicPartition tp : parsed.partitions()) {
                offsetsToAlter.put(tp, new OffsetAndMetadata(offset));
            }
        }
    }

    /**
     * Process --shift-by
     * @return true when the shift-by value results in a negative partition offset, otherwise false.
     */
    private boolean processShiftBy(Map<TopicPartition, OffsetAndMetadata> offsetsToAlter, Set<TopicPartition> groupPartitions, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        for (String spec : shiftBy) {
            var parsed = parseOffsetSpec(spec, groupPartitions, true);
            long shift = parsed.value();
            for (TopicPartition tp : parsed.partitions()) {
                long currentOffset = currentOffsets.get(tp).offset();
                long newOffset = currentOffset + shift;
                if (newOffset < 0) {
                    err().println("Error: Shift-by would result in negative offset for " + tp);
                    return true;
                }
                offsetsToAlter.put(tp, new OffsetAndMetadata(newOffset));
            }
        }
        return false;
    }

    /**
     * Process --to-datetime
     * @return true when the datetime format cannot be parsed, otherwise false.
     */
    private boolean processToDatetime(Admin admin, Map<TopicPartition, OffsetAndMetadata> offsetsToAlter, Set<TopicPartition> groupPartitions) {
        for (String spec : toDatetime) {
            var parsed = parseDatetimeSpec(spec, groupPartitions);
            String datetime = parsed.value();
            try {
                Instant instant = Instant.parse(datetime);
                long timestamp = instant.toEpochMilli();
                Map<TopicPartition, Long> timestampOffsets = getOffsetsForTimestamp(admin, parsed.partitions(), timestamp);
                for (Map.Entry<TopicPartition, Long> entry : timestampOffsets.entrySet()) {
                    offsetsToAlter.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                }
            } catch (DateTimeParseException _) {
                err().println("Error: Invalid ISO-8601 datetime format: " + datetime);
                err().println("Expected format: 2026-01-01T00:00:00Z");
                return true;
            }
        }

        return false;
    }

    /**
     * Process --by-duration
     * @return true when the duration format cannot be parsed, otherwise false.
     */
    private boolean processByDuration(Admin admin, Map<TopicPartition, OffsetAndMetadata> offsetsToAlter, Set<TopicPartition> groupPartitions, Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        var latestOffsets = getLatestOffsets(admin, groupPartitions);
        var currentTimestamps = getTimestampsForOffsets(currentOffsets, latestOffsets);

        for (String spec : byDuration) {
            var parsed = parseDatetimeSpec(spec, groupPartitions);
            String durationStr = parsed.value();
            try {
                Duration duration = Duration.parse(durationStr);
                var specs = parsed.partitions()
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), p -> OffsetSpec.forTimestamp(
                                currentTimestamps.get(p).plus(duration).toEpochMilli()
                        )));
                Map<TopicPartition, Long> timestampOffsets = getOffsets(admin, specs);

                for (Map.Entry<TopicPartition, Long> entry : timestampOffsets.entrySet()) {
                    var partition = entry.getKey();
                    var newOffset = entry.getValue();
                    if (newOffset >= 0) {
                        offsetsToAlter.put(partition, new OffsetAndMetadata(newOffset));
                    } else {
                        err().printf("""
                                Warning: no offset found when adjusting timestamp %s \
                                (for offset %d) by %s in partition %s. No change made to \
                                the committed offset for this partition.
                                """,
                                currentTimestamps.get(partition),
                                currentOffsets.get(partition).offset(),
                                duration,
                                partition);
                    }
                }
            } catch (DateTimeParseException _) {
                err().println("Error: Invalid ISO-8601 duration format: " + durationStr);
                err().println("Expected format: PT1H (1 hour), PT-1H (negative 1 hour), PT30M (30 minutes), etc.");
                return true;
            }
        }

        return false;
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
        } catch (NumberFormatException _) {
            throw new IllegalArgumentException("Invalid partition number: " + parts[1]);
        }
    }

    /**
     * Parse offset specification (offset=topic:partition)
     * @param spec The offset specification string
     * @param groupPartitions Available partitions in the group
     * @param allowNegative Whether to allow negative offset values (for shift-by)
     */
    private PartitionOffsetSpec<Long> parseOffsetSpec(String spec, Set<TopicPartition> groupPartitions, boolean allowNegative) {
        String[] parts = spec.split("=", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "Invalid format: expected 'offset=topic:partition', got: " + spec);
        }

        try {
            long offset = Long.parseLong(parts[0]);
            if (!allowNegative && offset < 0) {
                throw new IllegalArgumentException("Offset must be non-negative: " + offset);
            }

            Set<TopicPartition> partitions = parseTopicPartitionSpec(parts[1], groupPartitions);
            return new PartitionOffsetSpec<>(partitions, offset);
        } catch (NumberFormatException _) {
            throw new IllegalArgumentException("Invalid offset number: " + parts[0]);
        }
    }

    /**
     * Parse datetime/duration specification (datetime[=topic[:partition]])
     */
    private PartitionOffsetSpec<String> parseDatetimeSpec(String spec, Set<TopicPartition> groupPartitions) {
        String[] parts = spec.split("=", 2);
        String datetime = parts[0];
        Set<TopicPartition> partitions;

        if (parts.length == 2) {
            partitions = parseTopicPartitionSpec(parts[1], groupPartitions);
        } else {
            partitions = groupPartitions;
        }

        return new PartitionOffsetSpec<>(partitions, datetime);
    }

    private Map<TopicPartition, Long> getEarliestOffsets(Admin admin, Set<TopicPartition> partitions) {
        return getOffsets(admin, partitions, OffsetSpec.earliest());
    }

    private Map<TopicPartition, Long> getLatestOffsets(Admin admin, Set<TopicPartition> partitions) {
        return getOffsets(admin, partitions, OffsetSpec.latest());
    }

    private Map<TopicPartition, Long> getOffsetsForTimestamp(Admin admin, Set<TopicPartition> partitions, long timestamp) {
        return getOffsets(admin, partitions, OffsetSpec.forTimestamp(timestamp));
    }

    private Map<TopicPartition, Long> getOffsets(Admin admin, Set<TopicPartition> partitions, OffsetSpec spec) {
        var offsetSpecs = partitions.stream().collect(Collectors.toMap(Function.identity(), _ -> spec));
        return getOffsets(admin, offsetSpecs);
    }

    private Map<TopicPartition, Long> getOffsets(Admin admin, Map<TopicPartition, OffsetSpec> offsetSpecs) {
        ListOffsetsResult result = admin.listOffsets(offsetSpecs);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets = result.all()
                .toCompletionStage()
                .toCompletableFuture()
                .join();

        Map<TopicPartition, Long> offsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : offsets.entrySet()) {
            offsetMap.put(entry.getKey(), entry.getValue().offset());
        }
        return offsetMap;
    }

    private Map<TopicPartition, Instant> getTimestampsForOffsets(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets,
            Map<TopicPartition, Long> latestOffsets) {

        Map<TopicPartition, Instant> timestamps = HashMap.newHashMap(currentOffsets.size());
        Set<TopicPartition> assignments = new HashSet<>(currentOffsets.keySet());
        final Instant now = Instant.now();

        for (var offset : currentOffsets.entrySet()) {
            var partition = offset.getKey();

            if (offset.getValue().offset() >= latestOffsets.get(partition)) {
                timestamps.put(partition, now);
                assignments.remove(partition);
            }
        }

        if (assignments.isEmpty()) {
            return timestamps;
        }

        try (var consumer = clientFactory.createConsumer(contextName, Collections.emptyMap(), null)) {
            Instant deadline = Instant.now().plusSeconds(10);

            while (!assignments.isEmpty() && Instant.now().isBefore(deadline)) {
                consumer.assign(assignments);

                for (var partition : assignments) {
                    consumer.seek(partition, currentOffsets.get(partition).offset());
                }

                var timeout = Duration.between(Instant.now(), deadline);
                var result = consumer.poll(timeout.isNegative() ? Duration.ZERO : timeout);
                List<TopicPartition> missing = new ArrayList<>();

                assignments.forEach(topicPartition -> {
                    var records = result.records(topicPartition);

                    if (records.isEmpty()) {
                        missing.add(topicPartition);
                    } else {
                        var rec = records.get(0);
                        Instant ts = Instant.ofEpochMilli(rec.timestamp());
                        timestamps.put(topicPartition, ts);
                    }
                });

                assignments.retainAll(missing);
            }
        }

        // Check if all partitions were retrieved
        if (!assignments.isEmpty()) {
            throw new IllegalStateException(
                "Failed to retrieve timestamps for partitions: " + assignments +
                ". Timeout after 10 seconds.");
        }

        return timestamps;
    }

    private void printResults(String groupId, int alteredCount, int deletedCount) {
        // Print success message
        if (alteredCount > 0 && deletedCount > 0) {
            out().println("Altered offsets for " + alteredCount + " partition(s) and deleted offsets for " +
                    deletedCount + " partition(s) in group \"" + groupId + "\".");
        } else if (alteredCount > 0) {
            out().println("Altered offsets for " + alteredCount + " partition(s) in group \"" + groupId + "\".");
        } else if (deletedCount > 0) {
            out().println("Deleted offsets for " + deletedCount + " partition(s) from group \"" + groupId + "\".");
        }
    }

    private static record PartitionOffsetSpec<T>(Set<TopicPartition> partitions, T value) {}
}
