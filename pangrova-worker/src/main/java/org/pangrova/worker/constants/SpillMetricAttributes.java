package org.pangrova.worker.constants;

/** Stable domain and reason labels emitted by Worker spill and memory reclamation. */
public final class SpillMetricAttributes {
    private SpillMetricAttributes() {}

    /** Domain attribute value: spill triggered from Java-side the spill manager. */
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
}
