package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public abstract class ClikTestBase extends CommonTestBase {

    public static class Profile implements QuarkusTestProfile {

        private static final List<TestResourceEntry> RESOURCES =
                List.of(new TestResourceEntry(XdgConfigHomeManager.class));

        public Profile() {
            if (initialized.compareAndSet(false, true)) {
                try (ServerSocket serverSocket = new ServerSocket(0)) {
                    randomPort = serverSocket.getLocalPort();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                CommonTestBase.kafkaBootstrapServers = "localhost:" + randomPort;
            }
        }

        @Override
        public List<TestResourceEntry> testResources() {
            return RESOURCES;
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            Map<String, String> overrides = new HashMap<>(1);
            overrides.put("quarkus.kafka.devservices.port", String.valueOf(randomPort));
            return overrides;
        }
    }

    public static class XdgConfigHomeManager implements QuarkusTestResourceLifecycleManager {
        @Override
        public Map<String, String> start() {
            try {
                xdgConfigHome = Files.createTempDirectory("clik-test");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return Map.of("xdg.config.home", String.valueOf(xdgConfigHome));
        }

        @Override
        public void stop() {
            delete(xdgConfigHome);
        }
    }

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @Override
    public String kafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }
}
