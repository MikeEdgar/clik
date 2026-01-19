package io.streamshub.clik.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListStreamsGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.jboss.logging.Logger;

import io.streamshub.clik.kafka.model.CoordinatorInfo;
import io.streamshub.clik.kafka.model.GroupInfo;
import io.streamshub.clik.kafka.model.GroupMemberInfo;
import io.streamshub.clik.kafka.model.OffsetLagInfo;
import io.streamshub.clik.support.RootCause;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@ApplicationScoped
public class GroupService {

    private static final Logger LOGGER = Logger.getLogger(GroupService.class);

    /**
     * List all group IDs
     */
    public Collection<String> listGroupIds(Admin admin)
            throws ExecutionException, InterruptedException {
        return admin.listGroups().all().get()
                .stream()
                .map(GroupListing::groupId)
                .toList();
    }

    private Map<GroupType, Set<String>> groupsByType(Collection<GroupListing> listings) {
        return listings.stream()
                .map(listing -> Map.entry(listing.type().orElse(GroupType.UNKNOWN), listing.groupId()))
                .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toSet())));
    }

    private Map<GroupType, Set<String>> groupsByType(Admin admin) {
        return groupsByType(admin.listGroups().all()
                .toCompletionStage()
                .toCompletableFuture()
                .join());
    }

    private GroupType groupType(Admin admin, String groupId) {
        for (var entry : groupsByType(admin).entrySet()) {
            if (entry.getValue().contains(groupId)) {
                return entry.getKey();
            }
        }

        throw new GroupIdNotFoundException("No such group: " + groupId);
    }

    /**
     * List all groups, optionally filtered by type
     */
    public Collection<GroupInfo> listGroups(Admin admin, String typeFilter)
            throws ExecutionException, InterruptedException {

        // List all consumer groups
        Collection<GroupListing> listings = admin.listGroups()
                .all()
                .get()
                .stream()
                .filter(listing -> matchesType(listing, typeFilter))
                .toList();

        if (listings.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, GroupInfo.Builder> groups = HashMap.newHashMap(listings.size());

        for (GroupListing listing : listings) {
            String groupId = listing.groupId();
            GroupType groupType = listing.type().orElse(GroupType.UNKNOWN);

            groups.put(groupId, GroupInfo.builder()
                    .groupId(groupId)
                    .type(groupType.toString())
                    .protocol(listing.protocol())
                    .state(listing.groupState().orElse(GroupState.UNKNOWN).toString()));
        }

        var _ = groupsByType(listings)
                .entrySet()
                .stream()
                .map(entry -> describeGroups(admin, entry.getKey(), groups, entry.getValue(), true))
                .collect(collectingAndThen(toList(), Optional::of))
                .map(promises -> promises.toArray(CompletableFuture[]::new))
                .map(CompletableFuture::allOf)
                .map(CompletableFuture::join);

        return groups.values().stream().map(GroupInfo.Builder::build).toList();
    }

    private boolean matchesType(GroupListing listing, String typeFilter) {
        return typeFilter == null
                || typeFilter.isEmpty()
                || typeFilter.equalsIgnoreCase(listing.type().orElse(GroupType.UNKNOWN).toString());
    }

    private CompletableFuture<Void> describeGroups(Admin admin,
            GroupType type,
            Map<String, GroupInfo.Builder> groupData,
            Set<String> groupIds,
            boolean listing) {

        return switch (type) {
            case CLASSIC -> describeClassicGroups(admin, groupData, groupIds, listing);
            case CONSUMER -> describeConsumerGroups(admin, groupData, groupIds, listing);
            case SHARE -> describeShareGroups(admin, groupData, groupIds, listing);
            case STREAMS -> describeStreamsGroups(admin, groupData, groupIds, listing);
            default -> CompletableFuture.<Void>completedFuture(null);
        };
    }

    private CompletableFuture<Void> describeClassicGroups(Admin admin, Map<String, GroupInfo.Builder> groups, Set<String> groupIds, boolean listing) {
        return describeGroups(
                admin.describeClassicGroups(groupIds).describedGroups(),
                (group, builder) -> {
                    builder.groupId(group.groupId());
                    builder.state(group.state().toString());
                    builder.type(GroupType.CLASSIC.toString());
                    builder.protocol(group.protocol());
                    builder.coordinator(coordinator(group.coordinator()));
                    builder.memberCount(group.members().size());
                    if (!listing) {
                        builder.members(group.members()
                                .stream()
                                .map(member -> new GroupMemberInfo(
                                    member.consumerId(),
                                    member.clientId(),
                                    member.host(),
                                    assignments(member.assignment().topicPartitions().stream())))
                                .toList());
                    }
                },
                groups);
    }

    private CompletableFuture<Void> describeConsumerGroups(Admin admin, Map<String, GroupInfo.Builder> groups, Set<String> groupIds, boolean listing) {
        return describeGroups(
                admin.describeConsumerGroups(groupIds).describedGroups(),
                (group, builder) -> {
                    builder.groupId(group.groupId());
                    builder.state(group.groupState().toString());
                    builder.type(GroupType.CONSUMER.toString());
                    builder.coordinator(coordinator(group.coordinator()));
                    builder.memberCount(group.members().size());
                    if (!listing) {
                        builder.members(group.members()
                                .stream()
                                .map(member -> new GroupMemberInfo(
                                    member.consumerId(),
                                    member.clientId(),
                                    member.host(),
                                    assignments(member.assignment().topicPartitions().stream())))
                                .toList());
                    }
                },
                groups);
    }

    private CompletableFuture<Void> describeShareGroups(Admin admin, Map<String, GroupInfo.Builder> groups, Set<String> groupIds, boolean listing) {
        return describeGroups(
                admin.describeShareGroups(groupIds).describedGroups(),
                (group, builder) -> {
                    builder.groupId(group.groupId());
                    builder.state(group.groupState().toString());
                    builder.type(GroupType.SHARE.toString());
                    builder.coordinator(coordinator(group.coordinator()));
                    builder.memberCount(group.members().size());
                    if (!listing) {
                        builder.members(group.members()
                                .stream()
                                .map(member -> new GroupMemberInfo(
                                    member.consumerId(),
                                    member.clientId(),
                                    member.host(),
                                    assignments(member.assignment().topicPartitions().stream())))
                                .toList());
                    }
                },
                groups);
    }

    private CompletableFuture<Void> describeStreamsGroups(Admin admin, Map<String, GroupInfo.Builder> groups, Set<String> groupIds, boolean listing) {
        return describeGroups(
                admin.describeStreamsGroups(groupIds).describedGroups(),
                (group, builder) -> {
                    builder.groupId(group.groupId());
                    builder.state(group.groupState().toString());
                    builder.type(GroupType.STREAMS.toString());
                    builder.coordinator(coordinator(group.coordinator()));
                    builder.memberCount(group.members().size());
                    if (!listing) {
                        builder.members(group.members()
                                .stream()
                                .map(member -> new GroupMemberInfo(
                                    member.memberId(),
                                    member.clientId(),
                                    member.clientHost(),
                                    Collections.emptyList()))
                                .toList());
                    }
                },
                groups);
    }

    private <G> CompletableFuture<Void> describeGroups(
            Map<String, KafkaFuture<G>> descriptions,
            BiConsumer<G, GroupInfo.Builder> groupHandler,
            Map<String, GroupInfo.Builder> groups) {
        return CompletableFuture.allOf(descriptions.values()
                .stream()
                .map(f -> f.toCompletionStage().toCompletableFuture())
                .toArray(CompletableFuture[]::new))
            .handle((_, _) -> {
                for (Map.Entry<String, KafkaFuture<G>> entry : descriptions.entrySet()) {
                    KafkaFuture<G> value = entry.getValue();
                    GroupInfo.Builder groupBuilder = groups.get(entry.getKey());

                    if (value.isCompletedExceptionally()) {
                        var message = value.exceptionNow().toString();
                        LOGGER.infof("Failed to describe group %s: %s", entry.getKey(), message);
                        groupBuilder.describeError(value.exceptionNow().getMessage());
                    } else {
                        G group = value.toCompletionStage().toCompletableFuture().join();
                        groupHandler.accept(group, groupBuilder);
                    }
                }

                return null;
            });
    }

    private CoordinatorInfo coordinator(org.apache.kafka.common.Node coordinator) {
        return new CoordinatorInfo(
                coordinator.id(),
                coordinator.host(),
                coordinator.port()
        );
    }

    private List<GroupMemberInfo.PartitionAssignment> assignments(Stream<TopicPartition> assignments) {
        return assignments
                .collect(groupingBy(TopicPartition::topic, mapping(TopicPartition::partition, toList())))
                .entrySet()
                .stream()
                .map(e -> new GroupMemberInfo.PartitionAssignment(e.getKey(), e.getValue()))
                .toList();
    }

    /**
     * Describe a specific group with full details
     */
    public GroupInfo describeGroup(Admin admin, String groupId)
            throws ExecutionException, InterruptedException {

        Map<String, GroupInfo.Builder> groupData = Map.of(groupId, GroupInfo.builder());

        try {
            return describeGroups(admin, groupType(admin, groupId), groupData, Set.of(groupId), false)
                    .thenApply(_ -> groupData.get(groupId))
                    .join()
                    // Get offsets and lag for consumer groups
                    .offsets(getGroupOffsets(admin, groupId))
                    .build();
        } catch (Exception e) {
            // Handle GroupIdNotFoundException - group doesn't exist
            if (RootCause.of(e) instanceof GroupIdNotFoundException) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Get offset and lag information for a consumer group
     */
    private List<OffsetLagInfo> getGroupOffsets(Admin admin, String groupId)
            throws ExecutionException, InterruptedException {

        try {
            // Get current consumer group offsets
            Map<TopicPartition, OffsetAndMetadata> offsets = getGroupOffsetMap(admin, groupId);

            if (offsets.isEmpty()) {
                return Collections.emptyList();
            }

            // Get log end offsets for the same partitions
            Map<TopicPartition, OffsetSpec> endOffsetsMap = new HashMap<>();
            for (TopicPartition tp : offsets.keySet()) {
                endOffsetsMap.put(tp, OffsetSpec.latest());
            }

            ListOffsetsResult endOffsetsResult = admin.listOffsets(endOffsetsMap);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = endOffsetsResult.all().get();

            // Calculate lag
            List<OffsetLagInfo> lagInfoList = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long currentOffset = entry.getValue().offset();
                long logEndOffset = endOffsets.get(tp).offset();
                long lag = logEndOffset - currentOffset;

                OffsetLagInfo lagInfo = new OffsetLagInfo(
                        tp.topic(),
                        tp.partition(),
                        currentOffset,
                        logEndOffset,
                        lag
                );
                lagInfoList.add(lagInfo);
            }

            return lagInfoList;
        } catch (Exception _) {
            // Return empty list if offsets cannot be retrieved
            return Collections.emptyList();
        }
    }

    /**
     * Delete consumer groups
     *
     * @param admin Admin client
     * @param groupIds Collection of group IDs to delete
     * @throws ExecutionException if the operation fails
     * @throws InterruptedException if the operation is interrupted
     */
    public Map<String, Throwable> deleteGroups(Admin admin, Collection<String> groupIds)
            throws ExecutionException, InterruptedException {

        Map<String, Throwable> errors = new HashMap<>();
        var groupsByType = groupsByType(admin);

        for (var groupId : groupIds) {
            if (groupsByType.values().stream().noneMatch(groups -> groups.contains(groupId))) {
                errors.put(groupId, new GroupIdNotFoundException("No such group: " + groupId));
            }
        }

        var pending = groupsByType
            .entrySet()
            .stream()
            .map(entry -> Map.entry(entry.getKey(), entry.getValue().stream().filter(groupIds::contains).toList()))
            .filter(entry -> !entry.getValue().isEmpty())
            .map(entry -> switch (entry.getKey()) {
                    case CLASSIC, CONSUMER -> admin.deleteConsumerGroups(entry.getValue()).deletedGroups();
                    case SHARE -> admin.deleteShareGroups(entry.getValue()).deletedGroups();
                    case STREAMS -> admin.deleteStreamsGroups(entry.getValue()).deletedGroups();
                    default -> Collections.<String, KafkaFuture<Void>>emptyMap();
                })
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .map(entry -> Map.entry(entry.getKey(), entry.getValue().toCompletionStage().toCompletableFuture()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        CompletableFuture.allOf(pending.values().toArray(CompletableFuture[]::new))
            .thenRun(() -> pending.forEach((groupId, result) -> {
                if (result.isCompletedExceptionally()) {
                    errors.put(groupId, result.exceptionNow());
                } else {
                    LOGGER.debugf("Group %s was deleted without errors", groupId);
                }
            }))
            .join();

        return errors;
    }

    /**
     * Alter consumer group offsets
     *
     * @param admin Admin client
     * @param groupId Group ID
     * @param offsets Map of topic partitions to new offsets
     * @throws ExecutionException if the operation fails
     * @throws InterruptedException if the operation is interrupted
     */
    public void alterGroupOffsets(Admin admin, String groupId,
            Map<TopicPartition, OffsetAndMetadata> offsets)
            throws ExecutionException, InterruptedException {

        KafkaFuture<Void> promise = switch (groupType(admin, groupId)) {
            case CLASSIC, CONSUMER -> admin.alterConsumerGroupOffsets(groupId, offsets).all();
            case SHARE -> admin.alterShareGroupOffsets(groupId, offsets.entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset())))
                .all();
            case STREAMS -> admin.alterStreamsGroupOffsets(groupId, offsets).all();
            default -> KafkaFuture.completedFuture(null);
        };

        promise.toCompletionStage().toCompletableFuture().join();
    }

    /**
     * Delete consumer group offsets
     *
     * @param admin Admin client
     * @param groupId Group ID
     * @param partitions Set of topic partitions to delete
     * @throws ExecutionException if the operation fails
     * @throws InterruptedException if the operation is interrupted
     */
    public void deleteGroupOffsets(Admin admin, String groupId,
            Set<TopicPartition> partitions)
            throws ExecutionException, InterruptedException {

        KafkaFuture<Void> promise = switch (groupType(admin, groupId)) {
            case CLASSIC, CONSUMER -> admin.deleteConsumerGroupOffsets(groupId, partitions).all();
            case SHARE -> admin.deleteShareGroupOffsets(groupId, partitions.stream()
                    .map(TopicPartition::topic)
                    .distinct()
                    .collect(toSet()))
                .all();
            case STREAMS -> admin.deleteStreamsGroupOffsets(groupId, partitions).all();
            default -> KafkaFuture.completedFuture(null);
        };

        promise.toCompletionStage().toCompletableFuture().join();
    }

    /**
     * Get all topic partitions tracked by a consumer group
     *
     * @param admin Admin client
     * @param groupId Group ID
     * @return Set of topic partitions with committed offsets
     * @throws ExecutionException if the operation fails
     * @throws InterruptedException if the operation is interrupted
     */
    public Map<TopicPartition, OffsetAndMetadata> getGroupOffsetMap(Admin admin, String groupId)
            throws ExecutionException, InterruptedException {

        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> promise = switch (groupType(admin, groupId)) {
            case CLASSIC, CONSUMER -> admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata();
            case SHARE -> admin.listShareGroupOffsets(Map.of(groupId, new ListShareGroupOffsetsSpec().topicPartitions(null)))
                    .partitionsToOffsetAndMetadata(groupId);
            case STREAMS -> admin.listStreamsGroupOffsets(Map.of(groupId, new ListStreamsGroupOffsetsSpec()))
                    .partitionsToOffsetAndMetadata(groupId);
            default -> KafkaFuture.completedFuture(null);
        };

        return promise.toCompletionStage().toCompletableFuture().join();
    }

    /**
     * Check if group has active members
     *
     * @param admin Admin client
     * @param groupId Group ID
     * @return true if group has active members
     * @throws ExecutionException if the operation fails
     * @throws InterruptedException if the operation is interrupted
     */
    public boolean hasActiveMembers(Admin admin, String groupId)
            throws ExecutionException, InterruptedException {

        Map<String, GroupInfo.Builder> groupData = Map.of(groupId, GroupInfo.builder());

        return describeGroups(admin, groupType(admin, groupId), groupData, Set.of(groupId), true)
            .thenApply(_ -> groupData.get(groupId))
            .join()
            .build()
            .memberCount() > 0;
    }
}
