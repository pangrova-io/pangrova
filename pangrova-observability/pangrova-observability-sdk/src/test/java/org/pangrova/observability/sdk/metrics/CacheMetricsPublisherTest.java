package org.pangrova.observability.sdk.metrics;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.observability.api.noop.NoOpMetricsProvider;
import org.pangrova.observability.api.ObservableDoubleGauge;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests CacheStatsSnapshot hit rate calculations and CacheMetricsPublisher
 * gauge registration, publishing, and threshold warning.
 *
 * <p>Uses jqwik property tests for hit rate calculations (min 200 tries).
 * No mock frameworks — uses real NoOpMetricsProvider and CapturingMetricsProvider.
 *
 * Validates: Requirements 20.1, 20.2, 20.3, 20.4, 20.5
 */
class CacheMetricsPublisherTest {

    // -- CacheStatsSnapshot unit tests --

    @Test
    void snapshotRamHitRateWithZeroTotal() {
        var snapshot = buildSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.ramHitRate()).isEqualTo(0.0);
    }

    @Test
    void snapshotSsdHitRateWithZeroTotal() {
        var snapshot = buildSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.ssdHitRate()).isEqualTo(0.0);
    }

    @Test
    void snapshotOverallHitRateWithZeroTotal() {
        var snapshot = buildSnapshot(0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.overallHitRate()).isEqualTo(0.0);
    }

    @Test
    void snapshotRamHitRateCalculation() {
        var snapshot = buildSnapshot(80, 20, 0, 0, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.ramHitRate()).isCloseTo(0.8, within(1e-9));
    }

    @Test
    void snapshotSsdHitRateCalculation() {
        var snapshot = buildSnapshot(0, 0, 30, 70, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.ssdHitRate()).isCloseTo(0.3, within(1e-9));
    }

    @Test
    void snapshotOverallHitRateCalculation() {
        var snapshot = buildSnapshot(60, 20, 10, 10, 0, 0, 0, 0, 0, 0.0);
        // overall = (60+10) / (60+20+10+10) = 70/100 = 0.7
        assertThat(snapshot.overallHitRate()).isCloseTo(0.7, within(1e-9));
    }

    // -- CacheMetricsPublisher unit tests --

    @Test
    void gaugesRegisteredWithCorrectNames() {
        var capturing = new CapturingMetricsProvider();
        new CacheMetricsPublisher(capturing);

        assertThat(capturing.registeredGaugeNames).containsExactlyInAnyOrder(
                "pangrova.cache.ram_hit_rate",
                "pangrova.cache.ssd_hit_rate",
                "pangrova.cache.overall_hit_rate",
                "pangrova.cache.evictions",
                "pangrova.cache.ram_used_bytes",
                "pangrova.cache.ssd_used_bytes");
    }

    @Test
    void publishUpdatesAllGaugeValues() {
        var publisher = new CacheMetricsPublisher(new NoOpMetricsProvider());
        var snapshot = buildSnapshot(90, 10, 40, 60, 5, 2, 15, 4096, 8192, 0.9);

        publisher.publish(snapshot);

        assertThat(publisher.getRamHitRate()).isCloseTo(0.9, within(1e-9));
        assertThat(publisher.getSsdHitRate()).isCloseTo(0.4, within(1e-9));
        // overall = (90+40) / (90+10+40+60) = 130/200 = 0.65
        assertThat(publisher.getOverallHitRate()).isCloseTo(0.65, within(1e-9));
        assertThat(publisher.getEvictions()).isEqualTo(15.0);
        assertThat(publisher.getRamUsedBytes()).isEqualTo(4096.0);
        assertThat(publisher.getSsdUsedBytes()).isEqualTo(8192.0);
    }

    @Test
    void initialGaugeValuesAreZero() {
        var publisher = new CacheMetricsPublisher(new NoOpMetricsProvider());

        assertThat(publisher.getRamHitRate()).isEqualTo(0.0);
        assertThat(publisher.getSsdHitRate()).isEqualTo(0.0);
        assertThat(publisher.getOverallHitRate()).isEqualTo(0.0);
        assertThat(publisher.getEvictions()).isEqualTo(0.0);
        assertThat(publisher.getRamUsedBytes()).isEqualTo(0.0);
        assertThat(publisher.getSsdUsedBytes()).isEqualTo(0.0);
    }

    @Test
    void checkHitRateThresholdDoesNotThrow() {
        var publisher = new CacheMetricsPublisher(new NoOpMetricsProvider());
        var snapshot = buildSnapshot(50, 50, 0, 0, 0, 0, 0, 0, 0, 0.0);
        publisher.publish(snapshot);

        // Should log WARN but not throw
        publisher.checkHitRateThreshold(0.8);
    }

    @Test
    void checkHitRateThresholdNoWarnWhenAbove() {
        var publisher = new CacheMetricsPublisher(new NoOpMetricsProvider());
        var snapshot = buildSnapshot(90, 10, 0, 0, 0, 0, 0, 0, 0, 0.0);
        publisher.publish(snapshot);

        // Should not log WARN
        publisher.checkHitRateThreshold(0.5);
    }

    // -- jqwik property tests --

    /**
     * Property 1: RAM hit rate is always in [0.0, 1.0] for any non-negative inputs.
     * Validates: Requirements 20.3
     */
    @Property(tries = 200)
    void ramHitRateAlwaysBetweenZeroAndOne(
            @ForAll @LongRange(min = 0, max = 1_000_000) long hits,
            @ForAll @LongRange(min = 0, max = 1_000_000) long misses) {
        var snapshot = buildSnapshot(hits, misses, 0, 0, 0, 0, 0, 0, 0, 0.0);
        double rate = snapshot.ramHitRate();
        assertThat(rate).isBetween(0.0, 1.0);
    }

    /**
     * Property 2: SSD hit rate is always in [0.0, 1.0] for any non-negative inputs.
     * Validates: Requirements 20.3
     */
    @Property(tries = 200)
    void ssdHitRateAlwaysBetweenZeroAndOne(
            @ForAll @LongRange(min = 0, max = 1_000_000) long hits,
            @ForAll @LongRange(min = 0, max = 1_000_000) long misses) {
        var snapshot = buildSnapshot(0, 0, hits, misses, 0, 0, 0, 0, 0, 0.0);
        double rate = snapshot.ssdHitRate();
        assertThat(rate).isBetween(0.0, 1.0);
    }

    /**
     * Property 3: Overall hit rate is always in [0.0, 1.0] for any non-negative inputs.
     * Validates: Requirements 20.3
     */
    @Property(tries = 200)
    void overallHitRateAlwaysBetweenZeroAndOne(
            @ForAll @LongRange(min = 0, max = 1_000_000) long ramHits,
            @ForAll @LongRange(min = 0, max = 1_000_000) long ramMisses,
            @ForAll @LongRange(min = 0, max = 1_000_000) long ssdHits,
            @ForAll @LongRange(min = 0, max = 1_000_000) long ssdMisses) {
        var snapshot = buildSnapshot(
                ramHits, ramMisses, ssdHits, ssdMisses, 0, 0, 0, 0, 0, 0.0);
        double rate = snapshot.overallHitRate();
        assertThat(rate).isBetween(0.0, 1.0);
    }

    /**
     * Property 4: When all accesses are hits, hit rate equals 1.0.
     * Validates: Requirements 20.3
     */
    @Property(tries = 200)
    void allHitsYieldsRateOne(
            @ForAll @LongRange(min = 1, max = 1_000_000) long ramHits,
            @ForAll @LongRange(min = 1, max = 1_000_000) long ssdHits) {
        var snapshot = buildSnapshot(ramHits, 0, ssdHits, 0, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.overallHitRate()).isEqualTo(1.0);
    }

    /**
     * Property 5: When all accesses are misses, hit rate equals 0.0.
     * Validates: Requirements 20.3
     */
    @Property(tries = 200)
    void allMissesYieldsRateZero(
            @ForAll @LongRange(min = 1, max = 1_000_000) long ramMisses,
            @ForAll @LongRange(min = 1, max = 1_000_000) long ssdMisses) {
        var snapshot = buildSnapshot(0, ramMisses, 0, ssdMisses, 0, 0, 0, 0, 0, 0.0);
        assertThat(snapshot.overallHitRate()).isEqualTo(0.0);
    }

    /**
     * Property 6: publish() then read-back preserves snapshot values.
     * Validates: Requirements 20.3
     */
    @Property(tries = 200)
    void publishPreservesSnapshotValues(
            @ForAll @LongRange(min = 0, max = 1_000_000) long ramHits,
            @ForAll @LongRange(min = 0, max = 1_000_000) long ramMisses,
            @ForAll @LongRange(min = 0, max = 100_000) long evictions,
            @ForAll @LongRange(min = 0, max = 10_000_000) long ramUsed,
            @ForAll @LongRange(min = 0, max = 10_000_000) long ssdUsed) {
        var publisher = new CacheMetricsPublisher(new NoOpMetricsProvider());
        var snapshot = buildSnapshot(
                ramHits, ramMisses, 0, 0, 0, 0, evictions, ramUsed, ssdUsed, 0.0);

        publisher.publish(snapshot);

        assertThat(publisher.getEvictions()).isEqualTo((double) evictions);
        assertThat(publisher.getRamUsedBytes()).isEqualTo((double) ramUsed);
        assertThat(publisher.getSsdUsedBytes()).isEqualTo((double) ssdUsed);
    }

    // -- Helpers --

    private static CacheStatsSnapshot buildSnapshot(
            long ramHits, long ramMisses,
            long ssdHits, long ssdMisses,
            long promotions, long demotions, long evictions,
            long ramUsedBytes, long ssdUsedBytes,
            double prefetchHitRate) {
        return new CacheStatsSnapshot(
                ramHits, ramMisses, ssdHits, ssdMisses,
                promotions, demotions, evictions,
                ramUsedBytes, ssdUsedBytes, prefetchHitRate);
    }

    /**
     * Real MetricsProvider that captures registered gauge names.
     * Delegates to NoOpMetricsProvider for actual gauge instances.
     */
    private static final class CapturingMetricsProvider extends NoOpMetricsProvider {

        final List<String> registeredGaugeNames = new ArrayList<>();

        @Override
        public ObservableDoubleGauge createGauge(
                String name, String description, String unit,
                Supplier<Double> supplier) {
            registeredGaugeNames.add(name);
            return super.createGauge(name, description, unit, supplier);
        }
    }
}
