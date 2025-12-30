package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import io.streamshub.clik.config.ContextService;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

abstract class CommonTestBase {

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

    private Map<String, GroupType> allGroups(Admin admin) {
        return admin.listGroups()
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .join()
                .stream()
                .collect(Collectors.toMap(GroupListing::groupId, listing -> listing.type().orElse(GroupType.CLASSIC)));
    }

    public void deleteConsumerGroups() {
        try {
            Map<String, GroupType> allGroups = allGroups(admin);

            while (!allGroups.isEmpty()) {
                admin.deleteConsumerGroups(allGroups.keySet())
                    .deletedGroups()
                    .entrySet()
                    .stream()
                    .map(e -> {
                        return e.getValue().toCompletionStage().handle((nothing, error) -> {
                            if (error == null || error instanceof GroupIdNotFoundException) {
                                return (Void) null;
                            }

                            throw new CompletionException(error);
                        }).toCompletableFuture();
                    })
                    .reduce(CompletableFuture::allOf)
                    .orElseGet(() -> CompletableFuture.completedFuture(null))
                    .get(10, TimeUnit.SECONDS);

                allGroups = allGroups(admin);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
        }
    }

    @AfterAll
    static void complete() {
        delete(xdgConfigHome);
    }

    @AfterEach
    protected void tearDown() {
        // Close all consumers
        for (Consumer<String, String> consumer : consumers) {
            try {
                consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }

        consumers.clear();

        if (admin != null) {
            try {
                deleteConsumerGroups();
                deleteTopics();
            } finally {
                admin.close();
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
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);

        if (GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT).equals(groupProtocol)) {
            // Set very long session timeout to keep group alive during tests
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000"); // 60 seconds
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000"); // 20 seconds
        }

        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000"); // 120 seconds
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumers.add(consumer);

        return CompletableFuture.supplyAsync(() -> {
            consumer.subscribe(Collections.singleton(topic));

            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                consumer.poll(Duration.ofMillis(200));
                return isMember(groupId, consumer.groupMetadata().memberId());
            });

            return consumer;
        });
    }

    private boolean isMember(String groupId, String memberId) {
        try {
            var group = admin().describeConsumerGroups(Set.of(groupId))
                    .all()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .join()
                    .get(groupId);

            return switch(group.groupState()) {
                case UNKNOWN, DEAD, EMPTY -> false;
                default -> group.members().stream()
                    .anyMatch(m -> Objects.equals(memberId, m.consumerId()));
            };
        } catch (CompletionException e) {
            return false;
        }
    }
}
