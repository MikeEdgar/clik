package io.streamshub.clik.config;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.streamshub.clik.test.ClikTestBase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(ClikTestBase.Profile.class)
class ContextServiceTest extends ClikTestBase {

    @Inject
    ContextService contextService;

    @Test
    void testGetConfigDirectory() {
        Path configDir = contextService.getConfigDirectory();
        assertNotNull(configDir);
        assertEquals("clik", configDir.getFileName().toString());
    }

    @Test
    void testCreateContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test", config, false);

        assertTrue(contextService.contextExists("test"));
    }

    @Test
    void testCreateContextAlreadyExists() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test", config, false);

        assertThrows(IllegalArgumentException.class, () ->
                contextService.createContext("test", config, false));
    }

    @Test
    void testCreateContextWithOverwrite() {
        ContextConfig config1 = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        ContextConfig config2 = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9093")
                .build();

        contextService.createContext("test", config1, false);
        contextService.createContext("test", config2, true);

        Optional<ContextConfig> loaded = contextService.getContext("test");
        assertTrue(loaded.isPresent());
        assertEquals("localhost:9093", loaded.get().getCommon().get("bootstrap.servers"));
    }

    @Test
    void testListContexts() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("dev", config, false);
        contextService.createContext("staging", config, false);
        contextService.createContext("prod", config, false);

        List<String> contexts = contextService.listContexts();
        assertEquals(3, contexts.size());
        assertTrue(contexts.contains("dev"));
        assertTrue(contexts.contains("staging"));
        assertTrue(contexts.contains("prod"));
    }

    @Test
    void testListContextsEmpty() {
        List<String> contexts = contextService.listContexts();
        assertTrue(contexts.isEmpty());
    }

    @Test
    void testGetContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addCommon("security.protocol", "PLAINTEXT")
                .build();

        contextService.createContext("test", config, false);

        Optional<ContextConfig> loaded = contextService.getContext("test");
        assertTrue(loaded.isPresent());
        assertEquals("localhost:9092", loaded.get().getCommon().get("bootstrap.servers"));
        assertEquals("PLAINTEXT", loaded.get().getCommon().get("security.protocol"));
    }

    @Test
    void testGetContextNotFound() {
        Optional<ContextConfig> loaded = contextService.getContext("nonexistent");
        assertFalse(loaded.isPresent());
    }

    @Test
    void testDeleteContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test", config, false);
        assertTrue(contextService.contextExists("test"));

        contextService.deleteContext("test");
        assertFalse(contextService.contextExists("test"));
    }

    @Test
    void testDeleteContextNotFound() {
        assertThrows(IllegalArgumentException.class, () ->
                contextService.deleteContext("nonexistent"));
    }

    @Test
    void testDeleteCurrentContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test", config, false);
        contextService.setCurrentContext("test");

        assertEquals("test", contextService.getCurrentContext().orElse(null));

        contextService.deleteContext("test");

        assertFalse(contextService.getCurrentContext().isPresent());
    }

    @Test
    void testContextExists() {
        assertFalse(contextService.contextExists("test"));

        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test", config, false);
        assertTrue(contextService.contextExists("test"));
    }

    @Test
    void testGetCurrentContext() {
        Optional<String> current = contextService.getCurrentContext();
        assertFalse(current.isPresent());
    }

    @Test
    void testSetCurrentContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("test", config, false);
        contextService.setCurrentContext("test");

        Optional<String> current = contextService.getCurrentContext();
        assertTrue(current.isPresent());
        assertEquals("test", current.get());
    }

    @Test
    void testSetCurrentContextNotFound() {
        assertThrows(IllegalArgumentException.class, () ->
                contextService.setCurrentContext("nonexistent"));
    }

    @Test
    void testLoadContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addConsumer("group.id", "test-group")
                .addProducer("acks", "all")
                .build();

        contextService.createContext("test", config, false);

        ContextConfig loaded = contextService.loadContext("test");
        assertNotNull(loaded);
        assertEquals("localhost:9092", loaded.getCommon().get("bootstrap.servers"));
        assertEquals("test-group", loaded.getConsumer().get("group.id"));
        assertEquals("all", loaded.getProducer().get("acks"));
    }

    @Test
    void testLoadContextNotFound() {
        assertThrows(IllegalArgumentException.class, () ->
                contextService.loadContext("nonexistent"));
    }

    @Test
    void testSaveContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.saveContext("test", config);

        assertTrue(contextService.contextExists("test"));
        ContextConfig loaded = contextService.loadContext("test");
        assertEquals("localhost:9092", loaded.getCommon().get("bootstrap.servers"));
    }

    @Test
    void testRenameContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .addConsumer("group.id", "test-group")
                .build();

        contextService.createContext("old-name", config, false);

        contextService.renameContext("old-name", "new-name");

        assertFalse(contextService.contextExists("old-name"));
        assertTrue(contextService.contextExists("new-name"));

        ContextConfig loaded = contextService.loadContext("new-name");
        assertEquals("localhost:9092", loaded.getCommon().get("bootstrap.servers"));
        assertEquals("test-group", loaded.getConsumer().get("group.id"));
    }

    @Test
    void testRenameCurrentContext() {
        ContextConfig config = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();

        contextService.createContext("old-name", config, false);
        contextService.setCurrentContext("old-name");

        assertEquals("old-name", contextService.getCurrentContext().orElse(null));

        contextService.renameContext("old-name", "new-name");

        assertEquals("new-name", contextService.getCurrentContext().orElse(null));
    }

    @Test
    void testRenameContextNotFound() {
        assertThrows(IllegalArgumentException.class, () ->
                contextService.renameContext("nonexistent", "new-name"));
    }

    @Test
    void testRenameContextToExisting() {
        ContextConfig config1 = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9092")
                .build();
        ContextConfig config2 = ContextConfig.builder()
                .addCommon("bootstrap.servers", "localhost:9093")
                .build();

        contextService.createContext("context1", config1, false);
        contextService.createContext("context2", config2, false);

        assertThrows(IllegalArgumentException.class, () ->
                contextService.renameContext("context1", "context2"));
    }
}
