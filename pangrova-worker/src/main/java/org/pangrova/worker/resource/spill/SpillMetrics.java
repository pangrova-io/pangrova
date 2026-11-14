package org.pangrova.worker.resource.spill;

import org.pangrova.common.api.annotation.ThreadSafe;
import org.pangrova.observability.api.metrics.DoubleHistogram;
import org.pangrova.observability.api.metrics.LongCounter;
import org.pangrova.observability.api.metrics.MetricAttributes;
import org.pangrova.observability.api.metrics.MetricsProvider;
import org.pangrova.observability.api.metrics.ObservableDoubleGauge;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized recorder for spill / reclaim observability metrics.
 *
 * <p>This class is the single emitter for the spill subsystem. Both Java-side
 * {@link SpillManager} and the cross-domain coordinator (which receives native
 * arbitrator spill/shrink/oom events) record through this type so a single
 * Prometheus dashboard reflects events from both domains.
 *
 * <h2>Metrics emitted</h2>
 * <ul>
 *   <li>{@code pangrova.resource.spill.bytes} (counter, By) tagged
 *       {@code domain}, {@code reason}, {@code stage.id}, {@code tenant.id}</li>
 *   <li>{@code pangrova.resource.spill.files} (counter, {file}) tagged
 *       {@code domain}, {@code stage.id}</li>
 *   <li>{@code pangrova.resource.spill.duration} (histogram, s) tagged
 *       {@code domain}, {@code reason}</li>
 *   <li>{@code pangrova.resource.spill.reclaim.attempts} (counter, {attempt})</li>
 *   <li>{@code pangrova.resource.spill.reclaim.failures} (counter, {failure})</li>
 *   <li>{@code pangrova.resource.spill.directory.bytes} (gauge, By) per-directory</li>
 * </ul>
 *
 * <p>Attribute cardinality follows {@code docs/metrics-conventions.md}:
 * {@code stage.id} and {@code tenant.id} are bounded; {@code query.id} is
 * intentionally NOT a label (use trace exemplars for query correlation).
 *
 * <p>Thread-safe — counters and the directory-bytes ledger are concurrent.
 *
 * <p>Single-tier note: only on-disk spill is currently observable. Multi-tier
 * spill (S3/HDFS overflow) is deferred to a later architectural change; when
 * added, emit {@code pangrova.resource.spill.tier.bytes} with a {@code tier} label.
 */
@ThreadSafe
public final class SpillMetrics {

    /** Domain attribute value: spill triggered from Java-side {@link SpillManager}. */
    public static final String DOMAIN_JAVA = "java";
    /** Domain attribute value: spill reported from native (Velox) arbitrator. */
    public static final String DOMAIN_NATIVE = "native";

    /** Reason: triggered by Java-side memory pressure response. */
    public static final String REASON_PRESSURE = "memory_pressure";
    /** Reason: triggered by native arbitrator reclaim callback. */
    public static final String REASON_NATIVE_RECLAIM = "native_reclaim";
    /** Reason: triggered by native arbitrator shrink. */
    public static final String REASON_NATIVE_SHRINK = "native_shrink";
    /** Reason: out-of-memory event recorded for forensic counting. */
    public static final String REASON_OOM = "oom";
    /** Reason: no eligible candidates were available to spill. */
    public static final String REASON_NO_CANDIDATES = "no_candidates";
    /** Reason: candidates were selected but freed zero bytes. */
    public static final String REASON_RECLAIM_NO_PROGRESS = "no_progress";

    /** Recommended duration buckets for spill operations. */
    private static final List<Double> DURATION_BOUNDARIES_S =
            List.of(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0);

    private final MetricsProvider provider;

    private final LongCounter spillBytes;
    private final LongCounter spillFiles;
    private final DoubleHistogram spillDuration;
    private final LongCounter reclaimAttempts;
    private final LongCounter reclaimFailures;

    private final ConcurrentMap<String, AtomicLong> directoryBytes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ObservableDoubleGauge> directoryGauges = new ConcurrentHashMap<>();

    /**
     * Creates the spill metric instruments on the given metrics provider.
     */
    public SpillMetrics(MetricsProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
        this.spillBytes = provider.createCounter(
                "pangrova.resource.spill.bytes",
                "Total bytes spilled to disk by Java + native domains",
                "By");
        this.spillFiles = provider.createCounter(
                "pangrova.resource.spill.files",
                "Total spill files created",
                "{file}");
        this.spillDuration = provider.createHistogram(
                "pangrova.resource.spill.duration",
                "Wall-clock duration of a single spill operation",
                "s",
                DURATION_BOUNDARIES_S);
        this.reclaimAttempts = provider.createCounter(
                "pangrova.resource.spill.reclaim.attempts",
                "Reclaim attempts triggered against the spill manager",
                "{attempt}");
        this.reclaimFailures = provider.createCounter(
                "pangrova.resource.spill.reclaim.failures",
                "Reclaim attempts that produced no progress (zero bytes freed)",
                "{failure}");
    }

    /** Records a completed spill event. */
    public void recordSpill(String domain,
                            String reason,
                            String stageId,
                            String tenantId,
                            long bytes,
                            int files,
                            double durationSeconds) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative, got " + bytes);
        }
        if (files < 0) {
            throw new IllegalArgumentException("files must be non-negative, got " + files);
        }
        MetricAttributes byteAttrs = MetricAttributes.fromMap(Map.of(
                "domain", safe(domain, "unknown"),
                "reason", safe(reason, "unknown"),
                "stage.id", safe(stageId, "unknown"),
                "tenant.id", safe(tenantId, "unknown")));
        spillBytes.add(bytes, byteAttrs);
        if (files > 0) {
            MetricAttributes fileAttrs = MetricAttributes.of(
                    "domain", safe(domain, "unknown"),
                    "stage.id", safe(stageId, "unknown"));
            spillFiles.add(files, fileAttrs);
        }
        if (durationSeconds > 0) {
            MetricAttributes durAttrs = MetricAttributes.of(
                    "domain", safe(domain, "unknown"),
                    "reason", safe(reason, "unknown"));
            spillDuration.record(durationSeconds, durAttrs);
        }
    }

    /** Records a reclaim attempt against the spill manager. */
    public void recordReclaimAttempt(String pressure, String stageId) {
        reclaimAttempts.add(1, MetricAttributes.of(
                "pressure", safe(pressure, "unknown"),
                "stage.id", safe(stageId, "unknown")));
    }

    /** Records a reclaim attempt that produced no progress. */
    public void recordReclaimFailure(String pressure, String reason) {
        reclaimFailures.add(1, MetricAttributes.of(
                "pressure", safe(pressure, "unknown"),
                "reason", safe(reason, "unknown")));
    }

    /** Updates the current bytes-on-disk ledger for the given spill directory. */
    public void updateDirectoryBytes(String directory, long bytes) {
        Objects.requireNonNull(directory, "directory must not be null");
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must be non-negative, got " + bytes);
        }
        AtomicLong ledger = directoryBytes.computeIfAbsent(directory, k -> new AtomicLong(0));
        ledger.set(bytes);
        directoryGauges.computeIfAbsent(directory, k -> provider.createGauge(
                "pangrova.resource.spill.directory.bytes",
                "Bytes currently resident in a spill directory",
                "By",
                () -> {
                    AtomicLong ref = directoryBytes.get(k);
                    return ref == null ? 0.0 : (double) ref.get();
                }));
    }

    /** Returns the current per-directory ledger value (testing/diagnostics). */
    public long getDirectoryBytes(String directory) {
        AtomicLong ref = directoryBytes.get(directory);
        return ref == null ? 0L : ref.get();
    }

    private static String safe(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
