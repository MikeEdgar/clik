package io.streamshub.clik.kafka;

import io.streamshub.clik.config.ConfigurationLoader;
import io.streamshub.clik.config.ContextConfig;
import io.streamshub.clik.config.ContextService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;

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
}
