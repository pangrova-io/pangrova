package org.pangrova.planner.core.stability;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded LRU registry that captures optimizer plan choices for analysis:
 * {@code query_hash → recent (plan_hash, timestamp) records}.
 *
 * <p>Each {@link #record} call appends a new {@link PlanCaptureRecord} to the
 * deque for the given query hash. The deque is bounded by
 * {@link PlanStabilityConfig#maxPlansPerQuery()}; the oldest record is dropped
 * when the cap is exceeded. The set of <em>distinct query hashes</em> is also
 * bounded by {@link PlanStabilityConfig#maxCapturedQueries()} via an
 * access-order LRU map, so a long-running coordinator's memory footprint is
 * bounded regardless of how many distinct queries it has seen.</p>
 *
 * <p>Pragmatic scope (per G-CBO-06): this is plan <em>capture</em>. It records
 * what the optimizer chose so an operator can spot drift, build baselines, or
 * file a regression bug. It does NOT enforce a baseline or revert a plan —
 * those would require a fully serializable plan format and AST-level
 * substitution, deferred as future work.</p>
 *
 * <p>Thread safety: backed by a synchronized access-order
 * {@link LinkedHashMap}; safe for concurrent readers and writers.</p>
 */
public final class PlanCaptureRegistry {

    private final PlanStabilityConfig config;
    private final Clock clock;
    private final Map<String, Deque<PlanCaptureRecord>> entries;
    private final AtomicLong totalRecorded = new AtomicLong();
    private final AtomicLong distinctPlanShifts = new AtomicLong();

    public PlanCaptureRegistry() {
        this(PlanStabilityConfig.DEFAULT, Clock.systemUTC());
    }

    public PlanCaptureRegistry(PlanStabilityConfig config) {
        this(config, Clock.systemUTC());
    }

    /**
     * Creates a capture registry with an explicit clock for capture timestamps.
     *
     * @param config stability capture configuration
     * @param clock clock used to timestamp capture records
     */
    public PlanCaptureRegistry(PlanStabilityConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
        // Access-order LRU on the outer map so old query hashes evict first.
        this.entries = Collections.synchronizedMap(
                new LinkedHashMap<String, Deque<PlanCaptureRecord>>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(
                            Map.Entry<String, Deque<PlanCaptureRecord>> eldest) {
                        return super.size() > config.maxCapturedQueries();
                    }
                });
    }

    /**
     * Record an optimizer plan choice for a query. Returns the
     * {@link PlanCaptureRecord} that was added, or {@code null} when capture
     * is disabled (so callers can short-circuit follow-up logging).
     */
    public PlanCaptureRecord record(String queryHash, String planHash) {
        if (!config.captureEnabled() || config.maxCapturedQueries() == 0
                || config.maxPlansPerQuery() == 0) {
            return null;
        }
        PlanCaptureRecord record = new PlanCaptureRecord(planHash, clock.instant());
        synchronized (entries) {
            Deque<PlanCaptureRecord> deque = entries.computeIfAbsent(
                    queryHash, k -> new ArrayDeque<>());
            // Detect plan drift: the most recent record's plan hash differs
            // from the new one. Counted independently of size capping so the
            // metric reflects observed behaviour, not retention policy.
            PlanCaptureRecord previous = deque.peekLast();
            if (previous != null && !previous.planHash().equals(planHash)) {
                distinctPlanShifts.incrementAndGet();
            }
            deque.addLast(record);
            while (deque.size() > config.maxPlansPerQuery()) {
                deque.pollFirst();
            }
        }
        totalRecorded.incrementAndGet();
        return record;
    }

    /**
     * Returns the recent plan-hash records captured for the given query, in
     * chronological order (oldest first). Empty list when nothing has been
     * captured for the query.
     */
    public List<PlanCaptureRecord> recentPlans(String queryHash) {
        synchronized (entries) {
            Deque<PlanCaptureRecord> deque = entries.get(queryHash);
            return deque == null ? List.of() : new ArrayList<>(deque);
        }
    }

    /** Number of distinct query hashes currently retained. */
    public int size() {
        synchronized (entries) {
            return entries.size();
        }
    }

    /** Drop all captured records. */
    public void clear() {
        synchronized (entries) {
            entries.clear();
        }
    }

    /** Total number of plan choices recorded since startup. */
    public long totalRecorded() {
        return totalRecorded.get();
    }

    /**
     * Total number of times a freshly-recorded plan hash differed from the
     * previous one for the same query. Useful as a coarse drift indicator.
     */
    public long distinctPlanShifts() {
        return distinctPlanShifts.get();
    }

    public PlanStabilityConfig config() {
        return config;
    }
}
