package io.streamshub.clik.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;

/**
 * This handler observes application startup and shutdown events, blocking for a fixed
 * amount of time at shutdown until any tasks registered have completed. This supports
 * ensuring that connections are properly closed prior to process termination.
 */
@ApplicationScoped
public class LifecycleHandler {

    @Inject
    Logger logger;

    List<CompletableFuture<?>> promises = new ArrayList<>();

    boolean running;

    void startup(@Observes Startup event) {
        logger.debug("Starting up...");
        running = true;
    }

    void shutdown(@Observes Shutdown event) {
        logger.debug("Shutting down...");
        running = false;

        try {
            CompletableFuture.allOf(promises.toArray(CompletableFuture[]::new))
                .get(5, TimeUnit.SECONDS);
            logger.debug("All pending promises completed");
        } catch (InterruptedException e) {
            logger.debug("Interrupted before pending promises were complete.");
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            logger.debugf("Exception before pending promises were complete: %s", e.toString());
        }
    }

    public <T> CompletableFuture<T> supply(Supplier<T> task) {
        CompletableFuture<T> promise = CompletableFuture.supplyAsync(task);
        promises.add(promise);
        return promise;
    }

    public boolean isRunning() {
        return running;
    }
}
