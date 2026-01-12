package io.streamshub.clik.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFutureListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.jboss.logging.Logger;

class InternalAdminClient implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(InternalAdminClient.class);
    private static final int DEFAULT_CONNECTION_MAX_IDLE_MS = 9 * 60 * 1000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 100;
    private static final int DEFAULT_RECONNECT_BACKOFF_MS = 50;
    private static final int DEFAULT_RECONNECT_BACKOFF_MAX = 50;
    private static final int DEFAULT_SEND_BUFFER_BYTES = 128 * 1024;
    private static final int DEFAULT_RECEIVE_BUFFER_BYTES = 32 * 1024;

    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    private final ConsumerNetworkClient client;
    private final List<Node> bootstrapBrokers;

    static InternalAdminClient create(Properties props) {
        return create(new AbstractConfig(AdminClientConfig.configDef(), props, false));
    }

    static InternalAdminClient create(AbstractConfig config) {
        String clientId = "admin-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
        LogContext logContext = new LogContext("[LegacyAdminClient clientId=" + clientId + "] ");
        Time time = Time.SYSTEM;
        Metrics metrics = new Metrics(time);
        Metadata metadata = new Metadata(
                CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                60 * 60 * 1000L, logContext,
                new ClusterResourceListeners());
        metadata.bootstrap(ClientUtils.parseAndValidateAddresses(
                config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)));
        Selector selector = new Selector(
                DEFAULT_CONNECTION_MAX_IDLE_MS,
                metrics,
                time,
                "admin",
                ClientUtils.createChannelBuilder(config, time, logContext),
                logContext);
        NetworkClient networkClient = new NetworkClient(
                selector,
                metadata,
                clientId,
                DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                DEFAULT_RECONNECT_BACKOFF_MS,
                DEFAULT_RECONNECT_BACKOFF_MAX,
                DEFAULT_SEND_BUFFER_BYTES,
                DEFAULT_RECEIVE_BUFFER_BYTES,
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                time,
                true,
                new ApiVersions(),
                logContext,
                MetadataRecoveryStrategy.NONE);
        ConsumerNetworkClient highLevelClient = new ConsumerNetworkClient(
                logContext,
                networkClient,
                metadata,
                time,
                config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                Integer.MAX_VALUE);
        return new InternalAdminClient(highLevelClient, metadata.fetch().nodes());
    }

    InternalAdminClient(ConsumerNetworkClient client, List<Node> bootstrapBrokers) {
        this.client = client;
        this.bootstrapBrokers = bootstrapBrokers;
    }

    private <R extends AbstractResponse> CompletableFuture<R> send(Node target, AbstractRequest.Builder<?> request) {
        CompletableFuture<R> promise = new CompletableFuture<>();

        client.send(target, request).addListener(new RequestFutureListener<ClientResponse>() {
            @Override
            @SuppressWarnings("unchecked")
            public void onSuccess(ClientResponse value) {
                promise.complete((R) value.responseBody());
            }

            @Override
            public void onFailure(RuntimeException e) {
                promise.completeExceptionally(e);
            }
        });

        return promise;
    }

    @SuppressWarnings("unchecked")
    private <R extends AbstractResponse> R sendAnyNode(AbstractRequest.Builder<?> request) {
        for (Node broker : bootstrapBrokers) {
            try {
                return (R) send(broker, request).get(DEFAULT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (AuthenticationException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.debugf("Request %s failed against node %s", request.apiKey(), broker, e);
            }
        }
        throw new RuntimeException("Request " + request.apiKey() + " failed on brokers " + bootstrapBrokers);
    }

    protected CompletableFuture<NodeApiVersions> getNodeApiVersions(Node node) {
        return this.<ApiVersionsResponse>send(node, new ApiVersionsRequest.Builder())
            .thenApply(response -> {
                Errors error = Errors.forCode(response.data().errorCode());

                if (error.exception() != null) {
                    throw error.exception();
                } else {
                    return new NodeApiVersions(response.data().apiKeys(), response.data().supportedFeatures());
                }
            });
    }

    public void awaitBrokers() throws InterruptedException {
        List<Node> nodes;
        do {
            nodes = findAllBrokers();
            if (nodes.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(50);
            }
        } while (nodes.isEmpty());
    }

    private List<Node> findAllBrokers() {
        MetadataResponse response = sendAnyNode(MetadataRequest.Builder.allTopics());

        if (!response.errors().isEmpty()) {
            LOGGER.debugf("Metadata request contained errors: %s", response.errors());
        }

        return response.buildCluster().nodes();
    }

    public CompletableFuture<Map<Node, NodeApiVersions>> listAllBrokerVersionInfo() {
        var promise = new CompletableFuture<Map<Node, NodeApiVersions>>();
        var pending = findAllBrokers()
                .stream()
                .map(broker -> Map.entry(broker, getNodeApiVersions(broker)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        CompletableFuture.allOf(pending.values().toArray(CompletableFuture[]::new))
                .whenComplete((_, error) -> {
                    if (error != null) {
                        promise.completeExceptionally(error);
                    } else {
                        var result = HashMap.<Node, NodeApiVersions>newHashMap(pending.size());

                        for (var entry : pending.entrySet()) {
                            result.put(entry.getKey(), entry.getValue().join());
                        }

                        promise.complete(result);
                    }
                });

        return promise;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            LOGGER.error("Exception closing nioSelector:", e);
        }
    }
}