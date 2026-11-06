package org.pangrova.metadata.event;

import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.spi.event.MetadataEventListener;

import org.pangrova.common.lifecycle.ResourceCloser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous dispatcher for metadata events with bounded queue and fault isolation per listener.
 *
 * <p>Implements {@link AutoCloseable} to provide a clear resource lifecycle for the internal
 * thread pool. Calling {@link #close()} delegates to {@link #shutdown()} and is idempotent.
 */
public class MetadataEventDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataEventDispatcher.class);
    private static final int QUEUE_CAPACITY = 1024;
    private static final long SHUTDOWN_TIMEOUT_MS = 5000;

    private final List<MetadataEventListener> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService executor;
    private final AtomicLong eventsDropped = new AtomicLong(0);

    /** Creates a bounded single-threaded dispatcher. */
    public MetadataEventDispatcher() {
        this.executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAPACITY),
                r -> {
                    Thread t = new Thread(r, "metadata-event-dispatcher");
                    t.setDaemon(true);
                    return t;
                },
                (r, pool) -> {
                    long dropped = eventsDropped.incrementAndGet();
                    LOG.warn("event=metadata_event_rejected droppedTotal={}", dropped);
                });
    }

    public void addListener(MetadataEventListener listener) {
        listeners.add(listener);
    }

    public void removeListener(MetadataEventListener listener) {
        listeners.remove(listener);
    }

    public void dispatch(MetadataEvent event) {
        for (MetadataEventListener listener : listeners) {
            executor.submit(() -> invokeListener(listener, event));
        }
    }

    public long eventsDropped() {
        return eventsDropped.get();
    }

    @Override
    public void close() {
        shutdown();
    }

    public void shutdown() {
        ResourceCloser.shutdownExecutor(executor, Duration.ofMillis(SHUTDOWN_TIMEOUT_MS));
    }

    private void invokeListener(MetadataEventListener listener, MetadataEvent event) {
        try {
            listener.onEvent(event);
        } catch (Exception e) {
            LOG.error("event=metadata_listener_failed eventType={} listenerClass={}",
                    event.getClass().getSimpleName(),
                    listener.getClass().getSimpleName(), e);
        }
    }
}
