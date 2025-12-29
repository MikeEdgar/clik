package io.streamshub.clik.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public abstract class ClikMainTestBase extends CommonTestBase {

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
}
