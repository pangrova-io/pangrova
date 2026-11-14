package org.pangrova.planner.stability;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Configurable map: {@code query_hash → preferred_plan_hash}.
 *
 * <p>An operator declares "I know this query plans well as plan X" by calling
 * {@link #setBaseline(String, String)}. On every subsequent plan() of the
 * same query, the planner asks whether the freshly-optimized plan matches the
 * baseline (see {@link #compareAgainstBaseline(String, String)}). When it
 * doesn't, {@link PlanBaselineController} logs a warning carrying both the
 * baseline and observed plan hashes so the regression can be triaged.</p>
 *
 * <p><strong>Pragmatic scope (per G-CBO-06):</strong> this implementation
 * detects baseline drift and warns. It does <em>not</em> enforce or revert to
 * the baseline plan — true enforcement requires a fully serializable plan IR
 * and a way to substitute it back into the optimizer pipeline; that work is
 * deferred as future scope. The detect-and-warn semantics chosen here match
 * the staged rollout used by Oracle SPM (capture → baseline → enforce) where
 * enforce is the final, opt-in stage.</p>
 *
 * <p>Thread safety: backed by a {@link ConcurrentHashMap}.</p>
 */
public final class PlanBaselineRegistry {

    private final Map<String, String> baselines;
    private final AtomicLong matches = new AtomicLong();
    private final AtomicLong mismatches = new AtomicLong();
    private final AtomicLong noBaseline = new AtomicLong();

    public PlanBaselineRegistry() {
        this.baselines = new ConcurrentHashMap<>();
    }

    /**
     * Declare a preferred plan for a query. Replaces any previously-set
     * baseline for the same query hash. Pass {@code null} or empty to remove
     * the baseline.
     */
    public void setBaseline(String queryHash, String preferredPlanHash) {
        Objects.requireNonNull(queryHash, "queryHash");
        if (preferredPlanHash == null || preferredPlanHash.isEmpty()) {
            baselines.remove(queryHash);
        } else {
            baselines.put(queryHash, preferredPlanHash);
        }
    }

    /** Bulk loader: replaces the entire baseline map with the given entries. */
    public void loadBaselines(Map<String, String> entries) {
        Objects.requireNonNull(entries, "entries");
        this.baselines.clear();
        for (Map.Entry<String, String> e : entries.entrySet()) {
            if (e.getKey() != null && e.getValue() != null && !e.getValue().isEmpty()) {
                this.baselines.put(e.getKey(), e.getValue());
            }
        }
    }

    public Optional<String> getBaseline(String queryHash) {
        return Optional.ofNullable(baselines.get(queryHash));
    }

    public boolean removeBaseline(String queryHash) {
        return baselines.remove(queryHash) != null;
    }

    /**
     * Comparison verdict produced by
     * {@link #compareAgainstBaseline(String, String)}.
     *
     * <ul>
     *   <li>{@link #MATCH} — a baseline exists and the observed plan matches.</li>
     *   <li>{@link #MISMATCH} — a baseline exists and the observed plan
     *       differs (operator should triage).</li>
     *   <li>{@link #NO_BASELINE} — no baseline registered for this query.</li>
     * </ul>
     */
    public enum Verdict { MATCH, MISMATCH, NO_BASELINE }

    /**
     * Compare an observed plan hash to the registered baseline. Updates the
     * internal match/mismatch counters as a side effect so registries can
     * report regression rates over time.
     */
    public Verdict compareAgainstBaseline(String queryHash, String observedPlanHash) {
        Objects.requireNonNull(queryHash, "queryHash");
        Objects.requireNonNull(observedPlanHash, "observedPlanHash");
        String expected = baselines.get(queryHash);
        if (expected == null) {
            noBaseline.incrementAndGet();
            return Verdict.NO_BASELINE;
        }
        if (expected.equals(observedPlanHash)) {
            matches.incrementAndGet();
            return Verdict.MATCH;
        }
        mismatches.incrementAndGet();
        return Verdict.MISMATCH;
    }

    /** Snapshot of the current baseline map. */
    public Map<String, String> snapshot() {
        return Map.copyOf(baselines);
    }

    public int size() {
        return baselines.size();
    }

    public void clear() {
        baselines.clear();
    }

    public long matchCount() {
        return matches.get();
    }

    public long mismatchCount() {
        return mismatches.get();
    }

    public long noBaselineCount() {
        return noBaseline.get();
    }
}
