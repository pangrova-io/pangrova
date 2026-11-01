package org.pangrova.coordinator.task;

import java.util.concurrent.atomic.AtomicBoolean;

/** Holds one actual operation in the sole issued-query authority until its final exit. */
@org.pangrova.common.concurrent.ThreadSafe
public final class QueryIssuanceOperation implements org.pangrova.spi.connector.ConnectorQueryOperation {
    private final Runnable exited;
    private final AtomicBoolean closed = new AtomicBoolean();

    QueryIssuanceOperation(Runnable exited) { this.exited = exited; }

    @Override public void close() { if (closed.compareAndSet(false, true)) exited.run(); }
}
