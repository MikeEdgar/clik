package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.streamshub.clik.config.ContextService;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(Lifecycle.PER_CLASS)
abstract class CommonTestBase {

    private static final Logger LOGGER = Logger.getLogger(CommonTestBase.class);
    static AtomicBoolean initialized = new AtomicBoolean(false);
    static Path xdgConfigHome;
    static int randomPort;
    static String kafkaBootstrapServers;

    Admin admin;
    List<Consumer<String, String>> consumers = new ArrayList<>();

    protected static void delete(Path root) {
        if (root != null && Files.exists(root)) {
            try {
                Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    protected void deleteTopics() {
        try {
            var topicNames = admin.listTopics().names().toCompletionStage().toCompletableFuture().join();

            while (!topicNames.isEmpty()) {
                admin.deleteTopics(topicNames)
                    .topicNameValues()
                    .entrySet()
                    .stream()
                    .map(e -> {
                        return e.getValue().toCompletionStage().handle((nothing, error) -> {
                            if (error == null || error instanceof UnknownTopicOrPartitionException) {
                                return (Void) null;
                            }
                            throw new CompletionException(error);
                        }).toCompletableFuture();
                    })
                    .reduce(CompletableFuture::allOf)
                    .orElseGet(() -> CompletableFuture.completedFuture(null))
                    .get(10, TimeUnit.SECONDS);

                topicNames = admin.listTopics().names().toCompletionStage().toCompletableFuture().join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
        }
    }

    private Map<String, GroupListing> allGroups(Admin admin) {
        return admin.listGroups()
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .join()
                .stream()
                .collect(Collectors.toMap(GroupListing::groupId, Function.identity()));
    }

    private void removeLingeringMembers(Map<String, GroupListing> allGroups) {
        var nonEmptyGroups = allGroups.values()
                .stream()
                .filter(listing -> !listing.groupState().map(GroupState.EMPTY::equals).orElse(true))
                .map(GroupListing::groupId)
                .toList();

        var memberRemoval = admin.describeConsumerGroups(nonEmptyGroups)
            .all()
            .toCompletionStage()
            .toCompletableFuture()
            .join()
            .values()
            .stream()
            .map(group -> {
                var key = group.groupId();
                var value = group.members().stream()
                    .map(member -> member.groupInstanceId().orElse(""))
                    .filter(Predicate.not(String::isEmpty))
                    .map(MemberToRemove::new)
                    .toList();
                if (value.isEmpty()) {
                    return null;
                }
                return Map.entry(key, new RemoveMembersFromConsumerGroupOptions(value));
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        memberRemoval.forEach(admin::removeMembersFromConsumerGroup);
    }

    private void deleteConsumerGroups(Map<String, GroupListing> allGroups) throws Exception {
        admin.deleteConsumerGroups(allGroups.keySet())
            .deletedGroups()
            .entrySet()
            .stream()
            .map(e -> {
                return e.getValue().toCompletionStage().handle((nothing, error) -> {
                    if (error == null ||
                            error instanceof GroupIdNotFoundException ||
                            error instanceof GroupNotEmptyException) {
                        return (Void) null;
                    }

                    throw new CompletionException(error);
                }).toCompletableFuture();
            })
            .reduce(CompletableFuture::allOf)
            .orElseGet(() -> CompletableFuture.completedFuture(null))
            .get(10, TimeUnit.SECONDS);
    }

    public void deleteConsumerGroups() {
        try {
            Map<String, GroupListing> allGroups = allGroups(admin);

            while (!allGroups.isEmpty()) {
                removeLingeringMembers(allGroups);
                deleteConsumerGroups(allGroups);
                allGroups = allGroups(admin);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
        }
    }

    protected <K, V> void close(Consumer<K, V> consumer) {
        try {
            String groupId = consumer.groupMetadata().groupId();
            var removal = new MemberToRemove(consumer.groupMetadata().groupInstanceId().orElseThrow());
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            admin.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions(List.of(removal)));
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @BeforeAll
    void initialize() {
        LOGGER.infof("***** Before tests in %s", getClass().getName());
    }

    @AfterAll
    void complete() {
        delete(xdgConfigHome);
        LOGGER.infof("***** After tests in %s", getClass().getName());
    }

    @AfterEach
    protected void tearDown() {
        // Close all consumers
        for (Consumer<String, String> consumer : consumers) {
            close(consumer);
        }

        consumers.clear();

        if (admin != null) {
            try {
                deleteConsumerGroups();
                deleteTopics();
            } finally {
                admin.close();
                admin = null;
            }
        }

        // Clean up config directory always
        delete(configDir());
    }

    protected String kafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    protected Path xdgConfigHome() {
        return xdgConfigHome;
    }

    protected Path configDir() {
        return ContextService.getConfigDirectory(xdgConfigHome);
    }

    protected Admin admin() {
        if (admin == null) {
            Properties props = new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
            // Set very low values to reduce risk of out-dated metadata
            props.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "0");
            props.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0");
            admin = Admin.create(props);
        }

        return admin;
    }

    protected CompletableFuture<Consumer<String, String>> createConsumerGroup(String groupId, String topic) {
        return createConsumerGroup(groupId, topic, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT));
    }

    protected CompletableFuture<Consumer<String, String>> createConsumerGroup(String groupId, String topic, String groupProtocol) {
        String clientId = "test-client-" + UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, clientId);

        if (GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT).equals(groupProtocol)) {
            // Set very long session timeout to keep group alive during tests
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000"); // 60 seconds
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000"); // 20 seconds
        }

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000"); // 120 seconds
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumers.add(consumer);
        Instant beginTime = Instant.now();

        return CompletableFuture.supplyAsync(() -> {
            consumer.subscribe(Collections.singleton(topic));

            await().atMost(20, TimeUnit.SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return isMember(groupId, clientId, beginTime);
            });

            LOGGER.infof("Client instance %s took %s to become a member of group %s",
                    clientId,
                    Duration.between(beginTime, Instant.now()),
                    groupId);
            return consumer;
        });
    }

    private boolean isMember(String groupId, String clientId, Instant beginTime) {
        try {
            var group = admin().describeConsumerGroups(Set.of(groupId))
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

            return switch(group.groupState()) {
                case UNKNOWN, DEAD, EMPTY -> {
                    if (Duration.between(beginTime, Instant.now()).compareTo(Duration.ofSeconds(3)) > 0) {
                        LOGGER.debugf("State of group %s invalid: %s. Members: %s",
                                groupId, group.groupState(), allMembers);
                    }
                    yield false;
                }
                default -> {
                    if (allMembers.stream().anyMatch(memberId -> memberId.contains(clientId))) {
                        yield true;
                    }

                    if (Duration.between(beginTime, Instant.now()).compareTo(Duration.ofSeconds(3)) > 0) {
                        LOGGER.debugf("Group %s (state: %s) does not include clientId '%s'. Known members: %s",
                                groupId, group.groupState(), clientId, allMembers);
                    }

                    yield false;
                }
            };
        } catch (CompletionException e) {
            return false;
        }
    }
}
