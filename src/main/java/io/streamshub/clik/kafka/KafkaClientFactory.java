package io.streamshub.clik.kafka;

import io.streamshub.clik.config.ConfigurationLoader;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

@ApplicationScoped
public class KafkaClientFactory {

    private static final String FQCN_DESER = ByteArrayDeserializer.class.getName();
    private static final String FQCN_SER = ByteArraySerializer.class.getName();
    private static final String MSG_NO_CONTEXT = "No current context set. Use 'clik context use <name>' to set a context.";

    @Inject
    ContextService contextService;

    @Inject
    ConfigurationLoader configurationLoader;

    private String contextName(Optional<String> contextNameOption) {
        return contextNameOption
                .or(() -> contextService.getCurrentContext())
                .orElseThrow(() -> new IllegalStateException(MSG_NO_CONTEXT));
    }

    private Properties contextProperties(String contextName, KafkaClientType clientType, Map<String, String> overrides) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, clientType);
        overrides.forEach(props::setProperty);
        return props;
    }

    /**
     * Create AdminClient for the user-provided context or the current context
     * if none provided.
     */
    public Admin createAdminClient(Optional<String> contextNameOption) {
        return createAdminClient(contextName(contextNameOption));
    }

    /**
     * Create AdminClient from specific context
     */
    public Admin createAdminClient(String contextName) {
        return Admin.create(contextProperties(contextName, KafkaClientType.ADMIN, Collections.emptyMap()));
    }

    /**
     * Create KafkaProducer for the user-provided context or the current context
     * if none provided.
     */
    public Producer<byte[], byte[]> createProducer(Optional<String> contextNameOption, Map<String, String> properties) {
        return createProducer(contextName(contextNameOption), properties);
    }

    /**
     * Create KafkaProducer from specific context
     */
    public Producer<byte[], byte[]> createProducer(String contextName, Map<String, String> properties) {
        Properties props = contextProperties(contextName, KafkaClientType.PRODUCER, properties);

        // Set required serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, FQCN_SER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FQCN_SER);
        props.computeIfAbsent(
                ProducerConfig.CLIENT_ID_CONFIG,
                _ -> "clik-producer-" + UUID.randomUUID().toString()
        );

        return new KafkaProducer<>(props);
    }

    /**
     * Create KafkaConsumer for the user-provided context or the current context
     * if none provided.
     */
    public Consumer<byte[], byte[]> createConsumer(Optional<String> contextNameOption, Map<String, String> properties, String groupId) {
        return createConsumer(contextName(contextNameOption), properties, groupId);
    }

    /**
     * Create KafkaConsumer from specific context
     */
    public Consumer<byte[], byte[]> createConsumer(String contextName, Map<String, String> properties, String groupId) {
        Properties props = contextProperties(contextName, KafkaClientType.CONSUMER, properties);

        // Set required deserializers and group.id
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, FQCN_DESER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, FQCN_DESER);
        props.computeIfAbsent(
                ConsumerConfig.CLIENT_ID_CONFIG,
                _ -> "clik-consumer-" + UUID.randomUUID().toString()
        );

        if (groupId != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }

        return new KafkaConsumer<>(props);
    }
}
