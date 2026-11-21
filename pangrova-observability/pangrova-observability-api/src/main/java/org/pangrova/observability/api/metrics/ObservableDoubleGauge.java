package org.pangrova.observability.api.metrics;

import org.pangrova.common.api.annotation.Experimental;

/**
 * A gauge metric that reports a {@code double} value obtained from a
 * registered callback supplier.
 *
 * <p>Unlike counters and histograms, gauges are registration-based:
 * the value is pulled from the supplier at collection time rather than
 * pushed by the application. Consequently this interface exposes no
 * active recording methods. The registration owner must close this handle when
 * the observed resource is retired. Close is thread-safe and idempotent after
 * success; it unregisters this callback without closing the shared metrics backend.
 * A collection already in progress may finish after close, so suppliers must not
 * directly access a released native handle or another resource requiring a lease.
 */
@Experimental
public interface ObservableDoubleGauge extends AutoCloseable {
    /** Unregisters this observation. Failure is propagated and may be retried. */
    @Override
    void close();
}
