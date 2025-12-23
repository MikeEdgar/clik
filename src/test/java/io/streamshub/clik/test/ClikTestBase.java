package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import io.quarkus.test.junit.QuarkusTestProfile;
import io.streamshub.clik.config.ContextService;

public abstract class ClikTestBase {

    private static AtomicBoolean initialized = new AtomicBoolean(false);
    private static Path xdgConfigHome;
    private static int randomPort;
    private static String kafkaBootstrapServers;

    public static class Profile implements QuarkusTestProfile {
        public Profile() {
            if (initialized.compareAndSet(false, true)) {
                try (ServerSocket serverSocket = new ServerSocket(0)) {
                    randomPort = serverSocket.getLocalPort();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                kafkaBootstrapServers = "localhost:" + randomPort;

                try {
                    xdgConfigHome = Files.createTempDirectory("clik-test");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = new HashMap<>(2);
            overrides.put("xdg.config.home", String.valueOf(xdgConfigHome));
            overrides.put("quarkus.kafka.devservices.port", String.valueOf(randomPort));
            return overrides;
        }
    }

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

    @AfterAll
    static void complete() {
        delete(xdgConfigHome);
    }

    @AfterEach
    protected void tearDown() {
        // Clean up config directory always
        delete(configDir());
    }

    protected static String kafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    protected static Path xdgConfigHome() {
        return xdgConfigHome;
    }

    protected static Path configDir() {
        return ContextService.getConfigDirectory(xdgConfigHome);
    }
}
