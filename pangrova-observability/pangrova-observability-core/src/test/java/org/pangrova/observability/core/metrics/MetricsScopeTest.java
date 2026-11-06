package org.pangrova.observability.core.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.LongUpDownCounter;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;
import org.pangrova.observability.spi.metrics.MetricsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricsScopeTest {
    @Test
    void closesInReverseOrderContinuesAfterFailureAndRetriesOnlyFailedReleases() {
        var attempts = new ArrayList<String>();
        var failing = new AtomicInteger(1);
        var provider = new GaugeProvider(name -> () -> {
            attempts.add(name);
            if (!name.equals("first") && failing.get() > 0) {
                throw new IllegalStateException(name);
            }
        });
        var scope = new MetricsScope(provider);
        scope.createGauge("first", "", "", () -> 1.0);
        scope.createGauge("second", "", "", () -> 2.0);
        scope.createGauge("third", "", "", () -> 3.0);
        assertThatThrownBy(scope::close).isInstanceOf(IllegalStateException.class)
                .hasMessage("third").satisfies(failure ->
                        assertThat(failure.getSuppressed()).extracting(Throwable::getMessage).containsExactly("second"));
        assertThat(attempts).containsExactly("third", "second", "first");
        assertThatThrownBy(() -> scope.createGauge("late", "", "", () -> 0.0))
                .isInstanceOf(IllegalStateException.class);
        failing.set(0);
        scope.close();
        scope.close();
        assertThat(attempts).containsExactly("third", "second", "first", "third", "second");
        assertThatThrownBy(() -> scope.createCounter("late", "", ""))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void childAndIndividualReleaseDoNotCloseBorrowedParent() {
        var released = new ArrayList<String>();
        var parent = new MetricsScope(new GaugeProvider(name -> () -> released.add(name)));
        var child = new MetricsScope(parent);
        var first = child.createGauge("child", "", "", () -> 1.0);
        parent.createGauge("parent", "", "", () -> 2.0);
        first.close();
        first.close();
        child.close();
        assertThat(released).containsExactly("child");
        parent.createGauge("still-open", "", "", () -> 3.0);
        parent.close();
        assertThat(released).containsExactly("child", "still-open", "parent");
    }

    @Test
    void concurrentCreationCannotEscapeScopeClosure() throws Exception {
        var entered = new CountDownLatch(1);
        var finishCreation = new CountDownLatch(1);
        var closing = new CountDownLatch(1);
        var releases = new AtomicInteger();
        var scope = new MetricsScope(new GaugeProvider(name -> {
            entered.countDown();
            try {
                if (!finishCreation.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("creation timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return releases::incrementAndGet;
        }));
        var executor = Executors.newFixedThreadPool(2);
        try {
            var creation = executor.submit(() -> scope.createGauge("racing", "", "", () -> 1.0));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var close = executor.submit(() -> { closing.countDown(); scope.close(); });
            assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
            finishCreation.countDown();
            var registration = creation.get(5, TimeUnit.SECONDS);
            close.get(5, TimeUnit.SECONDS);
            registration.close();
            assertThat(releases).hasValue(1);
        } finally {
            finishCreation.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            scope.close();
        }
    }

    private record GaugeProvider(java.util.function.Function<String, ObservableDoubleGauge> factory)
            implements MetricsProvider {
        @Override public ObservableDoubleGauge createGauge(String n, String d, String u, Supplier<Double> s) {
            return factory.apply(n);
        }
        @Override public LongCounter createCounter(String n, String d, String u) { return value -> {}; }
        @Override public LongUpDownCounter createUpDownCounter(String n, String d, String u) { return value -> {}; }
        @Override public DoubleHistogram createHistogram(String n, String d, String u, List<Double> b) { return value -> {}; }
    }
}
