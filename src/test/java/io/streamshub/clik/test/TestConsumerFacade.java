package io.streamshub.clik.test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.jboss.logging.Logger;

import static org.awaitility.Awaitility.await;

public sealed interface TestConsumerFacade {

    String groupId();

    void seekToBeginning();

    void seekToEnd();

    void seek(Map<TopicPartition, Long> offsets);

    ConsumerRecords<String, String> poll(Duration duration);

    void commit();

    void close();

    public static CompletableFuture<? extends TestConsumerFacade> create(
            Admin admin,
            String bootstrapServers,
            GroupType type,
            GroupProtocol protocol,
            String groupId,
            String... topics) {

        return switch (type) {
        case CLASSIC, CONSUMER -> TestConsumer.create(admin, bootstrapServers, protocol, groupId, topics);
        case SHARE -> TestShareConsumer.create(admin, bootstrapServers, groupId, topics);
        case STREAMS -> TestStreamsConsumer.create(admin, bootstrapServers, groupId, topics);
        default -> throw new IllegalArgumentException("Unsupported group type: " + type);
    };
    }

    private static Properties initialProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private static Map<TopicPartition, Long> listTopicOffsets(Admin admin, Collection<String> topics, OffsetSpec spec) {
        var listLatestRequest = admin
                .describeTopics(topics)
                .allTopicNames()
                .toCompletionStage()
                .toCompletableFuture()
                .join()
                .values()
                .stream()
                .flatMap(topic -> topic.partitions()
                        .stream()
                        .map(partition -> new TopicPartition(topic.name(), partition.partition())))
                .collect(Collectors.toMap(Function.identity(), _ -> spec));

        return admin.listOffsets(listLatestRequest)
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .join()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private static Map<TopicPartition, Long> listPartitionOffsets(Admin admin, Collection<TopicPartition> partitions, OffsetSpec spec) {
        var listLatestRequest = partitions
                .stream()
                .collect(Collectors.toMap(Function.identity(), _ -> spec));

        return admin.listOffsets(listLatestRequest)
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .join()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    public static record TestConsumer(Admin admin, Properties properties, Consumer<String, String> consumer) implements TestConsumerFacade {
        private static final Logger LOGGER = Logger.getLogger(TestConsumer.class);

        public static CompletableFuture<TestConsumer> create(Admin admin, String bootstrapServers, GroupProtocol protocol, String groupId, String... topics) {
            String clientId = "test-consumer-" + UUID.randomUUID().toString();
            Properties props = initialProperties(bootstrapServers);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, clientId);

            if (groupId != null) {
                props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT));

                if (GroupProtocol.CLASSIC == protocol) {
                    // Set very long session timeout to keep group alive during tests
                    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000"); // 60 seconds
                    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000"); // 20 seconds
                }
            }

            var consumer = create(props);

            if (groupId != null) {
                Instant beginTime = Instant.now();
                consumer.subscribe(List.of(topics));

                return CompletableFuture.supplyAsync(() -> {
                    Set<TopicPartition> assignment = new HashSet<>();
                    Set<TopicPartition> assignable = Arrays.stream(topics)
                            .map(consumer::partitionsFor)
                            .flatMap(Collection::stream)
                            .map(p -> new TopicPartition(p.topic(), p.partition()))
                            .collect(Collectors.toSet());

                    consumer.subscribe(List.of(topics), new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            assignment.removeAll(partitions);
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            assignment.addAll(partitions);
                        }
                    });

                    await().atMost(10, TimeUnit.SECONDS).until(() -> {
                        consumer.poll(Duration.ofMillis(200));
                        return isAssignedMember(admin, groupId, clientId, assignment, assignable, beginTime);
                    });

                    LOGGER.infof("Client instance %s took %s to become a member of group %s with assignment %s, all assignable is %s",
                            clientId,
                            Duration.between(beginTime, Instant.now()),
                            groupId,
                            assignment,
                            assignable);

                    return new TestConsumer(admin, props, consumer);
                });
            } else {
                var assignment = Arrays.stream(topics)
                        .map(consumer::partitionsFor)
                        .flatMap(Collection::stream)
                        .map(p -> new TopicPartition(p.topic(), p.partition()))
                        .collect(Collectors.toSet());

                consumer.assign(assignment);
                assignment.forEach(consumer::position);

                return CompletableFuture.completedFuture(new TestConsumer(admin, props, consumer));
            }
        }

        private static boolean isAssignedMember(Admin admin,
                String groupId,
                String clientId,
                Set<TopicPartition> assignment,
                Set<TopicPartition> assignable,
                Instant beginTime) {

            try {
                var group = admin.describeConsumerGroups(Set.of(groupId))
                        .all()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join()
                        .get(groupId);
                var allMembers = group.members().stream()
                        .map(MemberDescription::groupInstanceId)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .toList();
                var allAssignments = group.members()
                        .stream()
                        .filter(member -> !member.groupInstanceId().orElse(clientId).contains(clientId))
                        .flatMap(member -> member.assignment().topicPartitions().stream())
                        .distinct()
                        .collect(Collectors.toSet());

                return switch(group.groupState()) {
                    case UNKNOWN, DEAD, EMPTY, RECONCILING, ASSIGNING -> {
                        if (Duration.between(beginTime, Instant.now()).compareTo(Duration.ofSeconds(3)) > 0) {
                            LOGGER.debugf("State of group %s invalid: %s. Members: %s",
                                    groupId, group.groupState(), allMembers);
                        }
                        yield false;
                    }
                    default -> {
                        if (allMembers.stream().anyMatch(memberId -> memberId.contains(clientId))) {
                            /*
                             * Either the member has an assignment or all of the partitions
                             * are assigned to other members.
                             */
                            LOGGER.debugf("Client %s of group %s [state=%s]. Assignment: %s ; all assignments: %s, assignable: %s",
                                    clientId,
                                    groupId,
                                    group.groupState(),
                                    assignment,
                                    allAssignments,
                                    assignable);
                            yield !assignment.isEmpty() || allAssignments.equals(assignable);
                        }

                        if (Duration.between(beginTime, Instant.now()).compareTo(Duration.ofSeconds(3)) > 0) {
                            LOGGER.debugf("Group %s (state: %s) does not include clientId '%s'. Known members: %s",
                                    groupId, group.groupState(), clientId, allMembers);
                        }

                        yield false;
                    }
                };
            } catch (CompletionException _) {
                return false;
            }
        }

        private static Consumer<String, String> create(Properties props) {
            return new KafkaConsumer<>(props);
        }

        @Override
        public String groupId() {
            return properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        }

        @Override
        public void seekToBeginning() {
            var assignment = consumer.assignment();
            consumer.seekToBeginning(assignment);
            assignment.forEach(consumer::position);
        }

        @Override
        public void seekToEnd() {
            var assignment = consumer.assignment();
            consumer.seekToEnd(assignment);
            assignment.forEach(consumer::position);
        }

        @Override
        public void seek(Map<TopicPartition, Long> offsets) {
            for (var entry : offsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration duration) {
            return consumer.poll(duration);
        }

        @Override
        public void commit() {
            consumer.commitSync();
        }

        @Override
        public void close() {
            try {
                String groupId = consumer.groupMetadata().groupId();
                var removal = new MemberToRemove(consumer.groupMetadata().groupInstanceId().orElseThrow());
                consumer.close(CloseOptions
                        .timeout(Duration.ofSeconds(5))
                        .withGroupMembershipOperation(GroupMembershipOperation.LEAVE_GROUP));
                admin.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions(List.of(removal)));
            } catch (Exception e) {
                LOGGER.debugf("Error closing consumer: %s", e.toString());
            }
        }
    }

    public static record TestShareConsumer(Admin admin, Properties properties, AtomicReference<ShareConsumer<String, String>> consumer) implements TestConsumerFacade {
        private static final Logger LOGGER = Logger.getLogger(TestShareConsumer.class);

        public static CompletableFuture<TestShareConsumer> create(Admin admin, String bootstrapServers, String groupId, String... topics) {
            Objects.requireNonNull(groupId, "groupId required");
            String clientId = "test-share-consumer-" + UUID.randomUUID().toString();
            Properties props = initialProperties(bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            props.put("share.acknowledgement.mode", "implicit");

            ShareConsumer<String, String> consumer = create(props, topics);
            Instant beginTime = Instant.now();

            return CompletableFuture.supplyAsync(() -> {
                await().atMost(100, TimeUnit.SECONDS).until(() -> {
                    // Set share.auto.offset.reset=earliest for the group
                    admin.incrementalAlterConfigs(Map.of(
                            new ConfigResource(ConfigResource.Type.GROUP, groupId),
                            Arrays.asList(new AlterConfigOp(new ConfigEntry(
                                    "share.auto.offset.reset", "earliest"),
                                    AlterConfigOp.OpType.SET))))
                            .all()
                            .toCompletionStage()
                            .toCompletableFuture()
                            .join();
                    return true;
                });

                await().atMost(100, TimeUnit.SECONDS).until(() -> {
                    var recs = consumer.poll(Duration.ofMillis(200));

                    var group = admin.describeShareGroups(List.of(groupId))
                        .all()
                        .toCompletionStage()
                        .toCompletableFuture()
                        .join()
                        .get(groupId);

                    var hasAssignment = group.members().stream()
                        .filter(m -> m.clientId().equals(clientId))
                        .findFirst()
                        .map(member -> !member.assignment().topicPartitions().isEmpty())
                        .orElse(false);

                    return group != null && group.groupState() == GroupState.STABLE && hasAssignment && !recs.isEmpty();
                });

                var commitResult = consumer.commitSync();

                LOGGER.infof("Client instance %s took %s to become a member of SHARE group %s. Commit result: %s",
                        clientId,
                        Duration.between(beginTime, Instant.now()),
                        groupId,
                        commitResult);

                return new TestShareConsumer(admin, props, new AtomicReference<>(consumer));
            });
        }

        private static ShareConsumer<String, String> create(Properties props, String... topics) {
            ShareConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaShareConsumer<>(props);
            consumer.subscribe(List.of(topics));
            return consumer;
        }

        @Override
        public String groupId() {
            return properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        }

        private void seek(Admin admin, OffsetSpec spec) {
            var subscription = consumer.get().subscription();
            consumer.get().close();

            var offsets = listTopicOffsets(admin, subscription, spec);

            admin.alterShareGroupOffsets(groupId(), offsets)
                    .all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            consumer.set(create(properties, subscription.toArray(String[]::new)));
        }

        @Override
        public void seekToBeginning() {
            seek(admin, OffsetSpec.earliest());
        }

        @Override
        public void seekToEnd() {
            seek(admin, OffsetSpec.latest());
        }

        @Override
        public void seek(Map<TopicPartition, Long> offsets) {
            var subscription = consumer.get().subscription();
            consumer.get().close();

            admin.alterShareGroupOffsets(groupId(), offsets)
                    .all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            consumer.set(create(properties, subscription.toArray(String[]::new)));
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration duration) {
            return consumer.get().poll(duration);
        }

        @Override
        public void commit() {
            consumer.get().commitSync();
        }

        @Override
        public void close() {
            try {
                consumer.get().close(Duration.ofSeconds(5));
            } catch (Exception e) {
                LOGGER.debugf("Error closing share consumer: %s", e.toString());
            }
        }
    }

    public static record TestStreamsConsumer(Admin admin, Properties properties, AtomicReference<KafkaStreams> streams) implements TestConsumerFacade {
        private static final Logger LOGGER = Logger.getLogger(TestStreamsConsumer.class);

        public static class StringSerde implements Serde<String> {
            @Override
            public Deserializer<String> deserializer() {
                return new StringDeserializer();
            }
            @Override
            public Serializer<String> serializer() {
                return new StringSerializer();
            }
        }

        public static CompletableFuture<TestStreamsConsumer> create(Admin admin, String bootstrapServers, String groupId, String... topics) {
            Objects.requireNonNull(groupId, "groupId required");
            Properties props = initialProperties(bootstrapServers);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
            props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupType.STREAMS.name().toLowerCase(Locale.ROOT));
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
            return CompletableFuture.completedFuture(new TestStreamsConsumer(admin, props, new AtomicReference<>(create(admin, props, topics))));
        }

        private static KafkaStreams create(Admin admin, Properties props, String... topics) {
            String outTopicName = topics[0] + "-out-" + UUID.randomUUID().toString();
            admin.createTopics(List.of(new NewTopic(outTopicName, 2, (short) 1)))
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .join();

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> input = builder.stream(topics[0]);

            input.filter((_, value) -> value != null)
                .mapValues(v -> v.toUpperCase(Locale.ROOT))
                .to(outTopicName);

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();
            return streams;
        }

        @Override
        public String groupId() {
            return properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG);
        }

        private void seek(Admin admin, OffsetSpec spec) {
            var subscription = streams.get().metadataForAllStreamsClients().stream()
                    .flatMap(meta -> meta.topicPartitions().stream())
                    .toList();
            streams.get().close();

            var offsets = listPartitionOffsets(admin, subscription, spec)
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));

            admin.alterStreamsGroupOffsets(groupId(), offsets)
                    .all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            streams.set(create(admin, properties, subscription.toArray(String[]::new)));
        }

        @Override
        public void seekToBeginning() {
            seek(admin, OffsetSpec.earliest());
        }

        @Override
        public void seekToEnd() {
            seek(admin, OffsetSpec.latest());
        }

        @Override
        public void seek(Map<TopicPartition, Long> offsets) {
            var subscription = streams.get().metadataForAllStreamsClients().stream()
                    .flatMap(meta -> meta.topicPartitions().stream())
                    .toList();
            streams.get().close();

            admin.alterStreamsGroupOffsets(groupId(), offsets.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue()))))
                    .all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join();

            streams.set(create(admin, properties, subscription.toArray(String[]::new)));
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration duration) {
            // No-op
            return ConsumerRecords.empty();
        }

        @Override
        public void close() {
            try {
                streams.get().close(Duration.ofSeconds(5));
            } catch (Exception e) {
                LOGGER.debugf("Error closing streams consumer: %s", e.toString());
            }
        }

        @Override
        public void commit() {
            // No-op
        }
    }

}
