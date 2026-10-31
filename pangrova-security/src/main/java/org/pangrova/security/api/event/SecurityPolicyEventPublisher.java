package org.pangrova.security.api.event;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-memory event bus for security policy change events.
 * Listener failures are isolated — one failing listener does not block others.
 *
 * <p>Concurrency strategy: CopyOnWriteArrayList for safe concurrent iteration.</p>
 */
@ThreadSafe
public class SecurityPolicyEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityPolicyEventPublisher.class);

    private final List<Consumer<SecurityPolicyChangeEvent>> listeners = new CopyOnWriteArrayList<>();

    /**
     * Registers a listener for security policy change events.
     *
     * @param listener the event consumer
     * @throws NullPointerException if listener is null
     */
    public void subscribe(Consumer<SecurityPolicyChangeEvent> listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        listeners.add(listener);
    }

    /**
     * Publishes an event to all registered listeners.
     *
     * @param event the security policy change event
     * @throws NullPointerException if event is null
     */
    public void publish(SecurityPolicyChangeEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        listeners.forEach(l -> notifySafely(l, event));
    }

    private void notifySafely(Consumer<SecurityPolicyChangeEvent> listener, SecurityPolicyChangeEvent event) {
        try {
            listener.accept(event);
        } catch (Exception e) {
            LOG.warn("action=notifyListener status=failed eventType={} error={}",
                    event.getClass().getSimpleName(), e.getMessage(), e);
        }
    }
}
