package org.pangrova.operations.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standard Operating Procedure framework for automated error recovery.
 * Routes errors to registered recovery actions or escalates unregistered ones.
 */
public final class RecoveryPlaybook {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryPlaybook.class);

    private final ConcurrentHashMap<String, RecoveryAction> registry = new ConcurrentHashMap<>();
    private final AtomicLong executedCounter = new AtomicLong(0);
    private final AtomicLong escalatedCounter = new AtomicLong(0);

    public void register(String errorCode, RecoveryAction action) {
        Objects.requireNonNull(errorCode, "errorCode");
        Objects.requireNonNull(action, "action");
        registry.put(errorCode, action);
    }

    /** Executes the registered recovery action for the error code, escalating on absence or failure. */
    public void handle(ErrorContext context) {
        String errorCode = context.errorCode();
        RecoveryAction action = registry.get(errorCode);
        if (action == null) {
            escalate(errorCode, context);
            return;
        }
        try {
            action.execute(context);
            executedCounter.incrementAndGet();
        } catch (Exception e) {
            LOG.error("event=recovery_action_failed errorCode={} error={}", errorCode, e.getMessage(), e);
            escalate(errorCode, context);
        }
    }

    /**
     * Returns true if an action is registered for the given error code.
     */
    public boolean hasAction(String errorCode) {
        return registry.containsKey(errorCode);
    }

    /**
     * Returns the number of successfully executed recovery actions.
     */
    public long executedCount() {
        return executedCounter.get();
    }

    /**
     * Returns the number of escalated (unhandled or failed) errors.
     */
    public long escalatedCount() {
        return escalatedCounter.get();
    }

    private void escalate(String errorCode, ErrorContext context) {
        escalatedCounter.incrementAndGet();
        LOG.warn("event=recovery_escalated errorCode={} message={}", errorCode, context.message());
    }
}
