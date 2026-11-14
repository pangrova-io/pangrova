package org.pangrova.observability.metrics;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.observability.api.metrics.MetricsProvider;

/**
 * Owns gauge registrations created through a borrowed provider. A process or component
 * closes its scope before releasing the observed resources and the shared backend.
 * Handles may be closed earlier; closing a child scope never closes its parent provider.
 *
 * <p>Creation and release are serialized. Once closing starts no new instruments may
 * be created. All gauges are attempted in reverse registration order; failed releases
 * remain owned for a subsequent close attempt. Existing counters and histograms retain
 * their backend lifetime. This class does not own the backend or any scheduler.</p>
 */
public final class MetricsScope implements MetricsProvider, AutoCloseable {
    private final MetricsProvider delegate;
    private final Set<Registration> registrations = new LinkedHashSet<>();
    private boolean closed;

    public MetricsScope(MetricsProvider delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public synchronized ObservableDoubleGauge createGauge(
            String name, String description, String unit, Supplier<Double> supplier) {
        requireOpen();
        var registration = new Registration(Objects.requireNonNull(
                delegate.createGauge(name, description, unit, supplier), "gauge registration"));
        registrations.add(registration);
        return registration;
    }

    @Override
    public synchronized LongCounter createCounter(String name, String description, String unit) {
        requireOpen();
        return delegate.createCounter(name, description, unit);
    }

    @Override
    public synchronized LongUpDownCounter createUpDownCounter(String name, String description, String unit) {
        requireOpen();
        return delegate.createUpDownCounter(name, description, unit);
    }

    @Override
    public synchronized DoubleHistogram createHistogram(
            String name, String description, String unit, List<Double> boundaries) {
        requireOpen();
        return delegate.createHistogram(name, description, unit, boundaries);
    }

    @Override
    public synchronized void close() {
        closed = true;
        Throwable failure = null;
        var pending = new ArrayList<>(registrations);
        for (int i = pending.size() - 1; i >= 0; i--) {
            try {
                pending.get(i).close();
            } catch (RuntimeException | Error problem) {
                if (failure == null) {
                    failure = problem;
                } else if (failure != problem) {
                    failure.addSuppressed(problem);
                }
            }
        }
        if (failure instanceof RuntimeException exception) throw exception;
        if (failure instanceof Error error) throw error;
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Metrics scope is closed");
    }

    private final class Registration implements ObservableDoubleGauge {
        private ObservableDoubleGauge gauge;

        private Registration(ObservableDoubleGauge gauge) {
            this.gauge = gauge;
        }

        @Override
        public void close() {
            synchronized (MetricsScope.this) {
                if (gauge == null) return;
                gauge.close();
                gauge = null;
                registrations.remove(this);
            }
        }
    }
}
