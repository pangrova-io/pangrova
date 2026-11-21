package org.pangrova.observability.api.metrics;

import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricAttributesContractTest {
    @Test
    void unimplementedDimensionsAreRejectedBeforeRecordingAnything() {
        var counterValue = new AtomicLong();
        var activeValue = new AtomicLong();
        var histogramValue = new AtomicReference<Double>();
        LongCounter counter = counterValue::addAndGet;
        LongUpDownCounter active = activeValue::addAndGet;
        DoubleHistogram histogram = histogramValue::set;
        var dimensions = MetricAttributes.of("tenant", "a");
        assertThatThrownBy(() -> counter.add(7, dimensions)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> active.add(-2, dimensions)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> histogram.record(3, dimensions)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(counterValue.get()).isZero();
        assertThat(activeValue.get()).isZero();
        assertThat(histogramValue.get()).isNull();
        counter.add(7, MetricAttributes.empty());
        active.add(-2, MetricAttributes.empty());
        histogram.record(3, MetricAttributes.empty());
        assertThat(counterValue.get()).isEqualTo(7);
        assertThat(activeValue.get()).isEqualTo(-2);
        assertThat(histogramValue.get()).isEqualTo(3.0);
    }
}
