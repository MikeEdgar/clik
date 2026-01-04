package io.streamshub.clik.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

@ApplicationScoped
public class ContextService {

    private static final String CONFIG_DIR_NAME = "clik";
    private static final String CONTEXTS_DIR_NAME = "contexts";
    private static final String CONFIG_FILE_NAME = "config.yaml";
    private static final String ROOT_CONFIG_FILE = "config.yaml";

    private final ObjectMapper yamlMapper;
    private final String xdgConfigHome;

    public static Path getConfigDirectory(Path baseDir) {
        return baseDir.resolve(CONFIG_DIR_NAME);
    }

    public ContextService(
            @ConfigProperty(name = "xdg.config.home", defaultValue = "${user.home}/.config")
            String xdgConfigHome) {
        YAMLFactory yamlFactory = YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build();
        this.yamlMapper = new ObjectMapper(yamlFactory);
        this.xdgConfigHome = xdgConfigHome;
    }

    public Path getConfigDirectory() {
        return getConfigDirectory(Paths.get(xdgConfigHome));
    }

    public void createContext(String name, ContextConfig config, boolean overwrite) {
        if (!overwrite && contextExists(name)) {
            throw new IllegalArgumentException("Context already exists: " + name);
        }

        saveContext(name, config);
    }

    public List<String> listContexts() {
        Path contextsDir = getConfigDirectory().resolve(CONTEXTS_DIR_NAME);

        if (!Files.exists(contextsDir)) {
            return List.of();
        }

        try (Stream<Path> paths = Files.list(contextsDir)) {
            return paths
                    .filter(Files::isDirectory)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list contexts", e);
        }
    }

    public Optional<ContextConfig> getContext(String name) {
        if (!contextExists(name)) {
            return Optional.empty();
        }

        try {
            return Optional.of(loadContext(name));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public void deleteContext(String name) {
        Path contextDir = getConfigDirectory().resolve(CONTEXTS_DIR_NAME).resolve(name);

        if (!Files.exists(contextDir)) {
            throw new IllegalArgumentException("Context does not exist: " + name);
        }

        try {
            // Delete config file
            Path configFile = contextDir.resolve(CONFIG_FILE_NAME);
            if (Files.exists(configFile)) {
                Files.delete(configFile);
            }

            // Delete directory
            Files.delete(contextDir);

            // If this was the current context, clear it
            Optional<String> currentContext = getCurrentContext();
            if (currentContext.isPresent() && currentContext.get().equals(name)) {
                clearCurrentContext();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete context: " + name, e);
        }
    }

    public void renameContext(String oldName, String newName) {
        Path contextsDir = getConfigDirectory().resolve(CONTEXTS_DIR_NAME);
        Path oldContextDir = contextsDir.resolve(oldName);
        Path newContextDir = contextsDir.resolve(newName);

        if (!Files.exists(oldContextDir)) {
            throw new IllegalArgumentException("Context does not exist: " + oldName);
        }

        if (Files.exists(newContextDir)) {
            throw new IllegalArgumentException("Context already exists: " + newName);
        }

        try {
            // Move directory
            Files.move(oldContextDir, newContextDir);

            // If this was the current context, update it
            Optional<String> currentContext = getCurrentContext();
            if (currentContext.isPresent() && currentContext.get().equals(oldName)) {
                setCurrentContext(newName);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to rename context: " + oldName, e);
        }
    }

    public boolean contextExists(String name) {
        Path contextDir = getConfigDirectory().resolve(CONTEXTS_DIR_NAME).resolve(name);
        Path configFile = contextDir.resolve(CONFIG_FILE_NAME);
        return Files.exists(configFile);
    }

    public Optional<String> getCurrentContext() {
        Path rootConfigFile = getConfigDirectory().resolve(ROOT_CONFIG_FILE);

        if (!Files.exists(rootConfigFile)) {
            return Optional.empty();
        }

        try {
            RootConfig rootConfig = yamlMapper.readValue(rootConfigFile.toFile(), RootConfig.class);
            return Optional.ofNullable(rootConfig)
                    .map(RootConfig::currentContext)
                    .filter(Predicate.not(String::isEmpty));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public void setCurrentContext(String name) {
        if (!contextExists(name)) {
            throw new IllegalArgumentException("Context does not exist: " + name);
        }

        Path configDir = getConfigDirectory();
        Path rootConfigFile = configDir.resolve(ROOT_CONFIG_FILE);

        try {
            // Ensure config directory exists
            ensureDirectoryExists(configDir);

            // Load or create root config
            final RootConfig updatedConfig;

            if (Files.exists(rootConfigFile)) {
                var currentConfig  = yamlMapper.readValue(rootConfigFile.toFile(), RootConfig.class);
                if (currentConfig == null) {
                    updatedConfig = new RootConfig(name);
                } else {
                    updatedConfig = currentConfig.withCurrentContext(name);
                }
            } else {
                updatedConfig = new RootConfig(name);
            }

            // Save
            yamlMapper.writeValue(rootConfigFile.toFile(), updatedConfig);

            // Set file permissions (600)
            setFilePermissions(rootConfigFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to set current context", e);
        }
    }

    public void clearCurrentContext() {
        Path rootConfigFile = getConfigDirectory().resolve(ROOT_CONFIG_FILE);

        if (!Files.exists(rootConfigFile)) {
            return;
        }

        try {
            RootConfig rootConfig = yamlMapper.readValue(rootConfigFile.toFile(), RootConfig.class);
            if (rootConfig != null) {
                yamlMapper.writeValue(rootConfigFile.toFile(), rootConfig.withCurrentContext(null));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clear current context", e);
        }
    }

    public ContextConfig loadContext(String name) {
        Path configFile = getConfigDirectory()
                .resolve(CONTEXTS_DIR_NAME)
                .resolve(name)
                .resolve(CONFIG_FILE_NAME);

        if (!Files.exists(configFile)) {
            throw new IllegalArgumentException("Context does not exist: " + name);
        }

        try {
            ContextConfig config = yamlMapper.readValue(configFile.toFile(), ContextConfig.class);
            if (config == null) {
                config = new ContextConfig();
            }
            return config;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load context: " + name, e);
        }
    }

    public void saveContext(String name, ContextConfig config) {
        Path contextDir = getConfigDirectory().resolve(CONTEXTS_DIR_NAME).resolve(name);
        Path configFile = contextDir.resolve(CONFIG_FILE_NAME);

        try {
            // Ensure directories exist
            ensureDirectoryExists(contextDir);

            // Write config file
            yamlMapper.writeValue(configFile.toFile(), config);

            // Set permissions: directory=700, file=600
            setDirectoryPermissions(contextDir);
            setFilePermissions(configFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to save context: " + name, e);
        }
    }

    private void ensureDirectoryExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
            setDirectoryPermissions(directory);
        }
    }

    private void setDirectoryPermissions(Path directory) {
        try {
            Set<PosixFilePermission> perms = Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE
            );
            Files.setPosixFilePermissions(directory, perms);
        } catch (UnsupportedOperationException | IOException e) {
            // Ignore on Windows or if POSIX not supported
        }
    }

    private void setFilePermissions(Path file) {
        try {
            Set<PosixFilePermission> perms = Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE
            );
            Files.setPosixFilePermissions(file, perms);
        } catch (UnsupportedOperationException | IOException e) {
            // Ignore on Windows or if POSIX not supported
        }
    }
}
