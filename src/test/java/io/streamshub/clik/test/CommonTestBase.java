package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import io.streamshub.clik.config.ContextService;

import static org.junit.jupiter.api.Assertions.fail;

abstract class CommonTestBase {

    static AtomicBoolean initialized = new AtomicBoolean(false);
    static Path xdgConfigHome;
    static int randomPort;
    static String kafkaBootstrapServers;

    Admin admin;

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

    @AfterAll
    static void complete() {
        delete(xdgConfigHome);
    }

    @AfterEach
    protected void tearDown() {
        if (admin != null) {
            try {
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
}
