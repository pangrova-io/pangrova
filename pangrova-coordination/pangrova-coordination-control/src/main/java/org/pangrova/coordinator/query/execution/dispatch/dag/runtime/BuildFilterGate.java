package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.protocol.v2.InListFilter;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
import org.pangrova.query.profiling.enums.RuntimeFilterOutcome;

/** Bounds the delay before one eligible probe enumerates its pinned splits. */
@ThreadSafe
public final class BuildFilterGate {
    public static final long MAX_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(200);
    static final int MAX_FILTERS_PER_QUERY = 4;
    private static final Comparator<StageRuntimeFilter> PRIORITY = Comparator
            .<StageRuntimeFilter>comparingLong(plan -> plan.probeRead().statistics().dataSize()).reversed()
            .thenComparing(Comparator.<StageRuntimeFilter>comparingLong(
                    plan -> plan.probeRead().statistics().sourceWorkUnits()).reversed())
            .thenComparingInt(StageRuntimeFilter::probeStageId)
            .thenComparingInt(StageRuntimeFilter::buildStageId);

    private static final ProbeRelease EMPTY = new ProbeRelease(RuntimeFilterOutcome.EMPTY_BUILD, Map.of());
    private static final ProbeRelease MISSING = new ProbeRelease(RuntimeFilterOutcome.MISSING_SUMMARY, null);
    private static final ProbeRelease TIMEOUT = new ProbeRelease(RuntimeFilterOutcome.TIMED_OUT, null);
    private static final ProbeRelease TERMINATED = new ProbeRelease(RuntimeFilterOutcome.QUERY_TERMINATED, null);

    private final StageRuntimeFilter plan;
    private final List<Integer> buildOrdinals;
    private final CompletableFuture<ProbeRelease> completedBuild = new CompletableFuture<>();
    private volatile ProbeRelease releasedProbe;
    private volatile long lateProbeEpoch;
    // The existing gate owns exactly one probe generation and its delivery claims.
    @javax.annotation.concurrent.GuardedBy("this")
    private final HashSet<String> deliveredAttempts = new HashSet<>();
    @javax.annotation.concurrent.GuardedBy("this")
    private RuntimeFilterBinding lateBinding;

    public record ProbeRelease(RuntimeFilterOutcome outcome, Map<Integer, List<BuildKeyRange>> ranges) {
        public ProbeRelease {
            ranges = ranges == null ? null : Map.copyOf(ranges);
        }

        public int rangeCount() {
            return ranges == null ? 0 : ranges.values().stream().mapToInt(List::size).sum();
        }
    }

    private BuildFilterGate(StageRuntimeFilter plan) {
        this.plan = plan;
        buildOrdinals = List.copyOf(plan.keys().stream()
                .map(StageRuntimeFilter.ProbeKey::buildOrdinal).distinct().sorted().toList());
    }

    /** Selects bounded, independent pairs in decreasing order of estimated probe I/O. */
    public static List<BuildFilterGate> forLiveQuery(StageDAG dag) {
        if (dag.runtimeFilters().isEmpty()) return List.of();
        var eligible = dag.runtimeFilters().stream()
                .filter(candidate -> eligible(dag, candidate))
                .sorted(PRIORITY)
                .toList();
        var selected = new ArrayList<BuildFilterGate>();
        var selectedStages = new HashSet<Integer>();
        for (var candidate : eligible) {
            if (selectedStages.contains(candidate.buildStageId())
                    || selectedStages.contains(candidate.probeStageId())) {
                continue;
            }
            selected.add(new BuildFilterGate(candidate));
            selectedStages.add(candidate.buildStageId());
            selectedStages.add(candidate.probeStageId());
            if (selected.size() == MAX_FILTERS_PER_QUERY) break;
        }
        return List.copyOf(selected);
    }

    private static boolean eligible(StageDAG dag, StageRuntimeFilter plan) {
        if (plan.buildStageId() == plan.probeStageId()
                || !dag.edges().getOrDefault(plan.buildStageId(), List.of()).isEmpty()) return false;
        var build = dag.findStage(plan.buildStageId()).plan();
        var edge = dag.findDownstreamEdge(plan.buildStageId());
        return edge != null && edge.exchangeId().equals(plan.buildExchangeId())
                && build.estimatedShuffleRows() >= 0
                && build.estimatedShuffleRows() <= StageRuntimeFilter.MAX_BUILD_ROWS
                && StageRuntimeFilter.enoughProbeWork(plan.probeRead().statistics());
    }

    public StageRuntimeFilter plan() { return plan; }

    public boolean isProbe(int stageId) { return stageId == plan.probeStageId(); }

    public boolean blocksProbe() { return releasedProbe == null; }

    public List<Integer> buildOrdinals() { return List.copyOf(buildOrdinals); }

    /** Called after the winning cohort has finished and any external publication is fenced. */
    public void onFinished(Stage stage) {
        if (stage.stageId() != plan.buildStageId() || !stage.exchangeReadyForConsumers()) return;
        var ranges = stage.completeBuildKeyRanges();
        if (ranges == null || buildOrdinals.size() != ranges.size()
                || !ranges.keySet().containsAll(buildOrdinals)) {
            completedBuild.complete(MISSING);
            return;
        }
        completedBuild.complete(ranges.values().stream().anyMatch(List::isEmpty)
                ? EMPTY : new ProbeRelease(RuntimeFilterOutcome.APPLIED, ranges));
    }

    /** Releases exactly one decision inside the common query-level wait window. */
    public CompletionStage<ProbeRelease> releaseProbe(CompletableFuture<?> queryCompletion,
            ScheduledExecutorService deadlines, long deadlineNanos) {
        var probeDecision = new CompletableFuture<ProbeRelease>();
        completedBuild.thenAccept(probeDecision::complete);
        queryCompletion.whenComplete((result, failure) -> completedBuild.complete(TERMINATED));
        var timeout = probeDecision.isDone() ? null : deadlines.schedule(
                () -> { probeDecision.complete(TIMEOUT); },
                Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
        return probeDecision.thenApply(decision -> {
            if (timeout != null) timeout.cancel(false);
            releasedProbe = decision;
            return decision;
        });
    }

    /** Takes the best available build evidence when the exact probe reaches split enumeration. */
    public Map<Integer, List<BuildKeyRange>> probeRanges(int stageId) {
        var released = releasedProbe;
        if (released == null || !isProbe(stageId)) return null;
        return (released == TIMEOUT ? completedBuild.getNow(TIMEOUT) : released).ranges();
    }

    /** Records the exact generation before transport can admit any of its unfiltered splits. */
    public synchronized void bindProbeGeneration(long epoch, boolean acceptsLateFilter) {
        lateProbeEpoch = acceptsLateFilter ? epoch : 0;
        deliveredAttempts.clear();
    }

    /** True only when both a matching unfiltered generation and complete nonempty domains exist. */
    public boolean lateDeliveryReady() {
        return lateProbeEpoch > 0 && completedBuild.getNow(TIMEOUT).outcome() == RuntimeFilterOutcome.APPLIED;
    }

    /** Claims one delivery to an exact attempt of the current unfiltered probe. */
    public synchronized Optional<RuntimeFilterBinding> claimLateBinding(TaskAttemptId attempt) {
        if (!lateDeliveryReady() || attempt.getPlanEpoch() != lateProbeEpoch
                || !isProbe(attempt.getStageId()) || !deliveredAttempts.add(attempt.getTaskId())) {
            return Optional.empty();
        }
        if (lateBinding == null) {
            var ranges = completedBuild.getNow(TIMEOUT).ranges();
            var binding = RuntimeFilterBinding.newBuilder().setSoleTableScan(true);
            for (var key : plan.keys()) {
                binding.addFilters(rowFilter(key.column(), ranges.get(key.buildOrdinal())));
            }
            lateBinding = binding.build();
        }
        return Optional.of(lateBinding);
    }

    private static RuntimeFilter rowFilter(String column, List<BuildKeyRange> ranges) {
        var filter = RuntimeFilter.newBuilder().setColumnName(column)
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_ROW_FILTER);
        if (ranges.stream().allMatch(range -> range.minimum() == range.maximum())) {
            var values = InListFilter.newBuilder();
            for (var range : ranges) values.addValues(intValue(range.minimum()));
            return filter.setInList(values).build();
        }
        // Each key is necessary. The enclosing interval preserves every match in its union.
        long lower = ranges.stream().mapToLong(BuildKeyRange::minimum).min().orElseThrow();
        long upper = ranges.stream().mapToLong(BuildKeyRange::maximum).max().orElseThrow();
        return filter.setRange(RangeFilter.newBuilder().setLower(intValue(lower)).setUpper(intValue(upper))
                .setLowerInclusive(true).setUpperInclusive(true)).build();
    }

    private static TypedValue intValue(long value) {
        return TypedValue.newBuilder().setInt64Value(value).build();
    }

    /** Indicates that the probe was released by the wait deadline. */
    public boolean releasedOnTimeout() {
        return releasedProbe == TIMEOUT;
    }
}
