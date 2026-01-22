package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
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
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.streamshub.clik.config.ContextService;
import io.streamshub.clik.kafka.GroupService;

import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class CommonTestBase {

    private static final Logger LOGGER = Logger.getLogger(CommonTestBase.class);
    static AtomicBoolean initialized = new AtomicBoolean(false);
    static Path xdgConfigHome;
    static int randomPort;
    static String kafkaBootstrapServers;

    Admin admin;
    List<TestConsumerFacade> consumers = new ArrayList<>();

    protected static void delete(Path root) {
        if (root != null && Files.exists(root)) {
            try {
                Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException _) {
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
                        return e.getValue().toCompletionStage().handle((_, error) -> {
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
        } catch (InterruptedException _) {
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
                .filter(listing -> List.of(GroupType.CONSUMER, GroupType.CLASSIC).contains(listing.type().orElse(GroupType.UNKNOWN)))
                .filter(listing -> !listing.groupState().map(GroupState.EMPTY::equals).orElse(true))
                .map(GroupListing::groupId)
                .toList();

        if (nonEmptyGroups.isEmpty()) {
            return;
        }

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
        GroupService groupService = new GroupService();

        groupService.deleteGroups(admin, allGroups.values()
                .stream()
                .map(GroupListing::groupId)
                .toList());
    }

    public void deleteConsumerGroups() {
        try {
            Map<String, GroupListing> allGroups = allGroups(admin);

            while (!allGroups.isEmpty()) {
                removeLingeringMembers(allGroups);
                deleteConsumerGroups(allGroups);
                allGroups = allGroups(admin);
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
        }
    }

    public void deleteAcls() {
        try {
            while (!admin.describeAcls(AclBindingFilter.ANY).values().get().isEmpty()) {
                var result = admin.deleteAcls(Collections.singleton(AclBindingFilter.ANY)).all().get();
                LOGGER.debugf("Deleted ACLs: %s", result);
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            fail(e);
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
        consumers.forEach(TestConsumerFacade::close);
        consumers.clear();

        try (var _ = admin()) {
            deleteAcls();
            deleteConsumerGroups();
            deleteTopics();
        } finally {
            admin = null;
        }

        // Clean up config directory always
        delete(configDir());
    }

    public String kafkaBootstrapServers() {
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

    protected CompletableFuture<? extends TestConsumerFacade> createConsumer(GroupType type, GroupProtocol protocol, String groupId, String... topics) {
        return TestConsumerFacade.create(admin(), kafkaBootstrapServers(), type, protocol, groupId, topics)
                .thenApply(consumer -> {
                    consumers.add(consumer);
                    return consumer;
                });
    }

    protected CompletableFuture<? extends TestConsumerFacade> createClassicConsumer(String groupId, String... topics) {
        return createConsumer(GroupType.CONSUMER, GroupProtocol.CLASSIC, groupId, topics);
    }
}
