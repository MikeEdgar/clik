package io.streamshub.clik.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;
import org.jboss.logging.Logger;

/**
 * Several completion candidate implementations for "best effort" to obtain
 * configuration keys for shell completion.
 */
public abstract class ConfigCandidates implements Iterable<String> {

    private final List<String> candidates;

    private ConfigCandidates(Collection<String> candidates) {
        this.candidates = candidates.stream().sorted().toList();
    }

    @Override
    public Iterator<String> iterator() {
        return candidates.iterator();
    }

    public static class Topic extends ConfigCandidates {
        private static final Logger LOGGER = Logger.getLogger(Topic.class);

        public Topic() {
            super(configNames());
        }

        /**
         * Since TopicConfig does not include a ConfigDef, we need to hack
         * this by using reflection. Fortunately, TopicConfig and all of the
         * constants are public access.
         */
        static List<String> configNames() {
            return Arrays.stream(TopicConfig.class.getDeclaredFields())
                .filter(field -> field.getName().endsWith("_CONFIG"))
                .map(field -> {
                    try {
                        return (String) field.get(TopicConfig.class);
                    } catch (IllegalArgumentException | IllegalAccessException _) {
                        LOGGER.debugf("Failed to obtain value of field TopicConfig.%s", field.getName());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        }
    }

    public static class AnyClient extends ConfigCandidates {
        public AnyClient() {
            super(configNames(
                AdminClientConfig.configDef(),
                ConsumerConfig.configDef(),
                ProducerConfig.configDef()
            ));
        }
    }

    public static class Admin extends ConfigCandidates {
        public Admin() {
            super(configNames(AdminClientConfig.configDef()));
        }
    }

    public static class Consumer extends ConfigCandidates {
        public Consumer() {
            super(configNames(ConsumerConfig.configDef()));
        }
    }

    public static class Producer extends ConfigCandidates {
        public Producer() {
            super(configNames(ProducerConfig.configDef()));
        }
    }

    static List<String> configNames(ConfigDef... baseConfigs) {
        return Arrays.stream(baseConfigs)
                .flatMap(base -> base.names().stream())
                .distinct()
                .toList();
    }
}
