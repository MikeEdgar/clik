package io.streamshub.clik.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;

import io.streamshub.clik.kafka.model.CoordinatorInfo;
import io.streamshub.clik.kafka.model.GroupInfo;
import io.streamshub.clik.kafka.model.GroupMemberInfo;
import io.streamshub.clik.kafka.model.OffsetLagInfo;

@ApplicationScoped
public class GroupService {

    /**
     * List all groups, optionally filtered by type
     */
    public Collection<GroupInfo> listGroups(Admin admin, String typeFilter)
            throws ExecutionException, InterruptedException {

        // List all consumer groups
        Collection<GroupListing> listings = admin.listGroups().all().get();

        if (listings.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, GroupInfo.Builder> groups = HashMap.newHashMap(listings.size());

        for (GroupListing listing : listings) {
            String groupId = listing.groupId();
            String groupType = determineGroupType(listing.type().orElse(GroupType.UNKNOWN));

            // Filter by type if specified
            if (typeFilter != null && !typeFilter.isEmpty() && !typeFilter.equalsIgnoreCase(groupType)) {
                continue;
            }

            groups.put(groupId, GroupInfo.builder()
                    .groupId(groupId)
                    .type(groupType)
                    .protocol(listing.protocol())
                    .state(listing.groupState().toString()));
        }

        Collection<String> groupIds = groups.keySet();
        // Describe groups to get state and member count
        Map<String, ConsumerGroupDescription> descriptions = admin.describeConsumerGroups(groupIds).all().get();

        for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
            groups.get(entry.getKey()).memberCount(entry.getValue().members().size());
        }

        return groups.values().stream().map(GroupInfo.Builder::build).toList();
    }

    /**
     * Describe a specific group with full details
     */
    public GroupInfo describeGroup(Admin admin, String groupId)
            throws ExecutionException, InterruptedException {

        ConsumerGroupDescription desc;
        try {
            Map<String, ConsumerGroupDescription> descriptions = admin.describeConsumerGroups(
                    Collections.singleton(groupId)).all().get();

            desc = descriptions.get(groupId);
            if (desc == null) {
                return null;
            }
        } catch (ExecutionException e) {
            // Handle GroupIdNotFoundException - group doesn't exist
            if (e.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) {
                return null;
            }
            throw e;
        }

        // Build coordinator info
        CoordinatorInfo coordinator = new CoordinatorInfo(
                desc.coordinator().id(),
                desc.coordinator().host(),
                desc.coordinator().port()
        );

        // Build member info
        List<GroupMemberInfo> members = new ArrayList<>();
        for (MemberDescription member : desc.members()) {
            Map<String, List<Integer>> topicPartitions = new HashMap<>();

            member.assignment().topicPartitions().forEach(tp -> {
                topicPartitions.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition());
            });

            List<GroupMemberInfo.PartitionAssignment> assignments = topicPartitions.entrySet().stream()
                    .map(e -> new GroupMemberInfo.PartitionAssignment(e.getKey(), e.getValue()))
                    .toList();

            GroupMemberInfo memberInfo = new GroupMemberInfo(
                    member.consumerId(),
                    member.clientId(),
                    member.host(),
                    assignments
            );
            members.add(memberInfo);
        }

        // Get offsets and lag for consumer groups
        List<OffsetLagInfo> offsets = null;

        switch (desc.type()) {
            case CLASSIC, CONSUMER:
                offsets = getGroupOffsets(admin, groupId);
                break;
            default:
                break;
        }

        return GroupInfo.builder()
                .groupId(desc.groupId())
                .type(determineGroupType(desc.type()))
                .state(desc.groupState().toString())
                .memberCount(desc.members().size())
                .coordinator(coordinator)
                .members(members)
                .offsets(offsets)
                .build();
    }

    /**
     * Get offset and lag information for a consumer group
     */
    private List<OffsetLagInfo> getGroupOffsets(Admin admin, String groupId)
            throws ExecutionException, InterruptedException {

        try {
            // Get current consumer group offsets
            ListConsumerGroupOffsetsResult offsetsResult = admin.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();

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
        } catch (Exception e) {
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
    public void deleteGroups(Admin admin, Collection<String> groupIds)
            throws ExecutionException, InterruptedException {
        admin.deleteConsumerGroups(groupIds)
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .get();
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
        admin.alterConsumerGroupOffsets(groupId, offsets)
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .get();
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
        admin.deleteConsumerGroupOffsets(groupId, partitions)
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .get();
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
        return admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();
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
        Map<String, ConsumerGroupDescription> descriptions = admin.describeConsumerGroups(
                Collections.singleton(groupId)).all().get();
        ConsumerGroupDescription desc = descriptions.get(groupId);
        return desc != null && !desc.members().isEmpty();
    }

    /**
     * Determine group type based on description
     */
    private String determineGroupType(GroupType type) {
        // For Kafka 4.1.1, use the GroupType enum
        // GroupType can be: UNKNOWN, CONSUMER, CLASSIC, SHARE, STREAMS
        if (type == GroupType.UNKNOWN) {
            // Default to consumer for unknown types
            return "consumer";
        }
        return type.toString().toLowerCase();
    }
}
