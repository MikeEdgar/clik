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

import java.util.Optional;
import java.util.Properties;

@ApplicationScoped
public class KafkaClientFactory {

    @Inject
    ContextService contextService;

    @Inject
    ConfigurationLoader configurationLoader;

    /**
     * Create AdminClient from current context
     */
    public Admin createAdminClient() {
        Optional<String> currentContext = contextService.getCurrentContext();
        if (!currentContext.isPresent()) {
            throw new IllegalStateException("No current context set. Use 'clik context use <name>' to set a context.");
        }
        return createAdminClient(currentContext.get());
    }

    /**
     * Create AdminClient from specific context
     */
    public Admin createAdminClient(String contextName) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, KafkaClientType.ADMIN);
        return Admin.create(props);
    }

    /**
     * Create KafkaProducer from current context
     */
    public Producer<byte[], byte[]> createProducer() {
        Optional<String> currentContext = contextService.getCurrentContext();
        if (!currentContext.isPresent()) {
            throw new IllegalStateException("No current context set. Use 'clik context use <name>' to set a context.");
        }
        return createProducer(currentContext.get());
    }

    /**
     * Create KafkaProducer from specific context
     */
    public Producer<byte[], byte[]> createProducer(String contextName) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, KafkaClientType.PRODUCER);

        // Set required serializers
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    /**
     * Create KafkaConsumer from current context
     */
    public Consumer<byte[], byte[]> createConsumer(String groupId) {
        Optional<String> currentContext = contextService.getCurrentContext();
        if (!currentContext.isPresent()) {
            throw new IllegalStateException("No current context set. Use 'clik context use <name>' to set a context.");
        }
        return createConsumer(currentContext.get(), groupId);
    }

    /**
     * Create KafkaConsumer from specific context
     */
    public Consumer<byte[], byte[]> createConsumer(String contextName, String groupId) {
        ContextConfig config = contextService.loadContext(contextName);
        Properties props = configurationLoader.mergeConfiguration(config, KafkaClientType.CONSUMER);

        // Set required deserializers and group.id
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new KafkaConsumer<>(props);
    }
}
