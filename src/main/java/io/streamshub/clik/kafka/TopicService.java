package io.streamshub.clik.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;

import io.streamshub.clik.command.topic.options.OffsetsOption;
import io.streamshub.clik.kafka.model.PartitionInfo;
import io.streamshub.clik.kafka.model.TopicInfo;

@ApplicationScoped
public class TopicService {

    /**
     * Create a new topic
     */
    public void createTopic(Admin admin, String name, int partitions, int replicationFactor, Map<String, String> configs) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(name, partitions, (short) replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            newTopic.configs(configs);
        }

        CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw new IllegalArgumentException("Topic \"" + name + "\" already exists.", e);
            }
            throw e;
        }
    }

    /**
     * List all topic names
     */
    public Set<String> listTopics(Admin admin, boolean includeInternal) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(includeInternal);
        ListTopicsResult result = admin.listTopics(options);
        return result.names().get();
    }

    /**
     * Get detailed information about a topic
     */
    public TopicInfo describeTopic(Admin admin, String name) throws ExecutionException, InterruptedException {
        return describeTopic(admin, name, Collections.emptyList());
    }

    /**
     * Get detailed information about a topic
     */
    public TopicInfo describeTopic(Admin admin, String name, List<OffsetsOption> offsets) throws ExecutionException, InterruptedException {
        Map<String, TopicInfo> topics = describeTopics(admin, Collections.singletonList(name), offsets);
        return topics.get(name);
    }

    /**
     * Get detailed information about multiple topics
     */
    public Map<String, TopicInfo> describeTopics(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        return describeTopics(admin, names, Collections.emptyList());
    }

    /**
     * Get detailed information about multiple topics
     */
    public Map<String, TopicInfo> describeTopics(Admin admin, Collection<String> names, List<OffsetsOption> offsetOptions) throws ExecutionException, InterruptedException {
        DescribeTopicsResult topicsResult = admin.describeTopics(names);
        Map<String, TopicDescription> descriptions = topicsResult.allTopicNames().get();

        // Get configurations for all topics
        List<ConfigResource> configResources = names.stream()
                .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                .toList();
        Map<ConfigResource, org.apache.kafka.clients.admin.Config> configs = admin.describeConfigs(configResources).all().get();

        List<Map<TopicPartition, ListOffsetsResultInfo>> offsetResults = Collections.emptyList();
        if (!offsetOptions.isEmpty()) {
            offsetResults = fetchOffsets(admin, descriptions, offsetOptions);
        }

        Map<String, TopicInfo> result = new HashMap<>();
        for (String name : names) {
            TopicDescription desc = descriptions.get(name);
            if (desc != null) {
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, name);
                org.apache.kafka.clients.admin.Config config = configs.get(configResource);

                // Calculate replication factor from first partition
                int replicationFactor = 0;
                if (!desc.partitions().isEmpty()) {
                    replicationFactor = desc.partitions().get(0).replicas().size();
                }

                TopicInfo topicInfo = TopicInfo.builder()
                        .name(desc.name())
                        .partitions(desc.partitions().size())
                        .replicationFactor(replicationFactor)
                        .internal(desc.isInternal())
                        .config(convertConfig(config))
                        .partitionDetails(convertPartitions(desc, offsetOptions, offsetResults))
                        .build();

                result.put(name, topicInfo);
            }
        }

        return result;
    }

    /**
     * Alter topic configuration
     */
    public void alterTopicConfig(Admin admin, String name, Map<String, String> configs, List<String> deleteConfigs) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);

        List<AlterConfigOp> ops = new ArrayList<>();

        // Add SET operations for new/updated configs
        if (configs != null && !configs.isEmpty()) {
            configs.entrySet().stream()
                    .map(entry -> new AlterConfigOp(
                            new ConfigEntry(entry.getKey(), entry.getValue()),
                            AlterConfigOp.OpType.SET))
                    .forEach(ops::add);
        }

        // Add DELETE operations for configs to remove
        if (deleteConfigs != null && !deleteConfigs.isEmpty()) {
            deleteConfigs.stream()
                    .map(key -> new AlterConfigOp(
                            new ConfigEntry(key, null),
                            AlterConfigOp.OpType.DELETE))
                    .forEach(ops::add);
        }

        if (!ops.isEmpty()) {
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = Collections.singletonMap(resource, ops);
            admin.incrementalAlterConfigs(alterConfigs).all().get();
        }
    }

    /**
     * Increase partition count for a topic
     */
    public void increasePartitions(Admin admin, String name, int newTotalCount) throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitions = Collections.singletonMap(
            name,
            NewPartitions.increaseTo(newTotalCount)
        );
        CreatePartitionsResult result = admin.createPartitions(newPartitions);
        result.all().get();
    }

    /**
     * Delete a single topic
     */
    public void deleteTopic(Admin admin, String name) throws ExecutionException, InterruptedException {
        deleteTopics(admin, Collections.singletonList(name));
    }

    /**
     * Delete multiple topics
     */
    public void deleteTopics(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        DeleteTopicsResult result = admin.deleteTopics(names);
        result.all().get();
    }

    private List<Map<TopicPartition, ListOffsetsResultInfo>> fetchOffsets(
            Admin admin,
            Map<String, TopicDescription> descriptions,
            List<OffsetsOption> offsetOptions) {

        Set<TopicPartition> allPartitions = descriptions.values()
                .stream()
                .flatMap(topic -> topic.partitions().stream().map(partition -> new TopicPartition(topic.name(), partition.partition())))
                .distinct()
                .collect(Collectors.toSet());

        var offsetPromises = offsetOptions.stream()
                .map(OffsetsOption::spec)
                .map(spec -> {
                    var offsetsRequest = allPartitions.stream()
                            .map(partition -> Map.entry(partition, spec))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    return admin.listOffsets(offsetsRequest).all().toCompletionStage().toCompletableFuture();
                })
                .toList();

        return CompletableFuture.allOf(offsetPromises.toArray(CompletableFuture[]::new))
            .thenApply(_ -> offsetPromises.stream().map(CompletableFuture::join).toList())
            .join();
    }

    private Map<String, String> convertConfig(org.apache.kafka.clients.admin.Config config) {
        if (config == null) {
            return Collections.emptyMap();
        }

        return config.entries().stream()
                .filter(entry -> entry.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG)
                .collect(Collectors.toMap(
                        ConfigEntry::name,
                        entry -> entry.value() != null ? entry.value() : ""
                ));
    }

    private List<PartitionInfo> convertPartitions(TopicDescription desc,
            List<OffsetsOption> offsetOptions,
            List<Map<TopicPartition, ListOffsetsResultInfo>> offsetResults) {

        if (desc.partitions() == null) {
            return Collections.emptyList();
        }

        List<PartitionInfo> partitions = new ArrayList<>();
        for (org.apache.kafka.common.TopicPartitionInfo kafkaPartition : desc.partitions()) {
            var partitionKey = new TopicPartition(desc.name(), kafkaPartition.partition());

            List<Long> offsetValues = offsetResults.stream()
                    .map(e -> e.get(partitionKey))
                    .map(o -> o != null ? o.offset() : null)
                    .toList();

            List<PartitionInfo.OffsetInfo> offsets = null;

            if (!offsetValues.isEmpty()) {
                offsets = new ArrayList<>(offsetValues.size());
                for (int i = 0; i < offsetValues.size(); i++) {
                    offsets.add(new PartitionInfo.OffsetInfo(offsetOptions.get(i).option(), offsetValues.get(i)));
                }
            }

            PartitionInfo partition = new PartitionInfo(
                    kafkaPartition.partition(),
                    kafkaPartition.leader() != null ? kafkaPartition.leader().id() : -1,
                    kafkaPartition.replicas().stream().map(Node::id).toList(),
                    kafkaPartition.isr().stream().map(Node::id).toList(),
                    offsets
            );
            partitions.add(partition);
        }

        return partitions;
    }
}
