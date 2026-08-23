package io.streamshub.clik.support;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;

public class Futures {

    private Futures() {
        // No instances
    }

    public static <T> T join(KafkaFuture<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }

    public static <T> T get(KafkaFuture<T> future, Duration waitLimit) {
        try {
            return future.get(waitLimit.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new CompletionException(e);
        }
    }
}
