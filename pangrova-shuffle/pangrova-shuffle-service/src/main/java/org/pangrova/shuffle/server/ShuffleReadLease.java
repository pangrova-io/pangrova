package org.pangrova.shuffle.server;

import org.pangrova.common.concurrency.NotThreadSafe;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;

import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Keeps local read admission within the durable successor barrier for this reader. */
@NotThreadSafe
final class ShuffleReadLease implements AutoCloseable {
    static final long WINDOW_MILLIS = 1_000L;
    private static final long CLOCK_GUARD_MILLIS = 50L;

    private final ShuffleLifecycleStore store;
    private final String queryId;
    private final ExecutionOwnerIdentity owner;
    private final UUID pin;
    private final Clock clock;
    private long validUntilNanos;
    private long validUntilMillis;
    private boolean closed;
    private boolean released;

    ShuffleReadLease(ShuffleLifecycleStore store, String queryId, ExecutionOwnerIdentity owner,
                     ShuffleLifecycleStore.ReadPin grant, Clock clock, long startedNanos, long startedMillis) {
        this.store = store;
        this.queryId = queryId;
        this.owner = owner;
        this.pin = grant.id();
        this.clock = clock;
        setLocalDeadline(startedNanos, startedMillis, grant.protectedForMillis());
    }

    void requireLive(long nowMillis) {
        if (closed) throw new SecurityException("shuffle read pin is closed or expired");
        long startedNanos = System.nanoTime();
        if (unexpired(startedNanos, nowMillis)) return;
        setLocalDeadline(startedNanos, nowMillis, store.grantReadWindow(queryId, owner, pin));
        if (!unexpired(System.nanoTime(), clock.millis())) {
            throw new SecurityException("shuffle read authority window expired during admission");
        }
    }

    private boolean unexpired(long nowNanos, long nowMillis) {
        return validUntilNanos - nowNanos > 0 && validUntilMillis > nowMillis;
    }

    private void setLocalDeadline(long startedNanos, long startedMillis, long protectedForMillis) {
        // Start before the database round trip. Wall elapsed time also catches a suspended host.
        long allowance = Math.max(0, protectedForMillis - CLOCK_GUARD_MILLIS);
        validUntilNanos = startedNanos + TimeUnit.MILLISECONDS.toNanos(allowance);
        validUntilMillis = Math.addExact(startedMillis, allowance);
    }

    @Override
    public void close() {
        closed = true;
        if (!released) {
            store.releaseRead(pin);
            released = true;
        }
    }
}
