package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
import org.pangrova.query.profiling.OperatorStatsSource.RuntimeFilterOutcome;

/** Bounds the delay before the single eligible probe root enumerates its pinned splits. */
public final class BuildFilterGate {
    private static final long MAX_WAIT_MS = 200;

    private static final ProbeRelease EMPTY = new ProbeRelease(RuntimeFilterOutcome.EMPTY_BUILD, Map.of());
    private static final ProbeRelease MISSING = new ProbeRelease(RuntimeFilterOutcome.MISSING_SUMMARY, null);
    private static final ProbeRelease TIMEOUT = new ProbeRelease(RuntimeFilterOutcome.TIMED_OUT, null);
    private static final ProbeRelease TERMINATED = new ProbeRelease(RuntimeFilterOutcome.QUERY_TERMINATED, null);

    private final StageRuntimeFilter plan;
    private final List<Integer> buildOrdinals;
    private final CompletableFuture<ProbeRelease> completedBuild = new CompletableFuture<>();
    private volatile ProbeRelease releasedProbe;

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
        buildOrdinals = plan.keys().stream()
                .map(StageRuntimeFilter.ProbeKey::buildOrdinal).distinct().sorted().toList();
    }

    /** Enables the gate only for a bounded build and a probe with enough potential I/O savings. */
    public static BuildFilterGate create(StageDAG dag) {
        var plan = dag.runtimeFilter().orElse(null);
        if (plan == null) return null;
        var build = dag.findStage(plan.buildStageId()).plan();
        var edge = dag.findDownstreamEdge(plan.buildStageId());
        if (edge == null || !edge.exchangeId().equals(plan.buildExchangeId())
                || build.estimatedShuffleRows() < 0
                || build.estimatedShuffleRows() > StageRuntimeFilter.MAX_BUILD_ROWS) {
            return null;
        }
        var probe = plan.probeRead().statistics();
        return StageRuntimeFilter.enoughProbeWork(probe) ? new BuildFilterGate(plan) : null;
    }

    public StageRuntimeFilter plan() { return plan; }

    public boolean isProbe(int stageId) { return stageId == plan.probeStageId(); }

    public boolean blocks(int stageId) { return isProbe(stageId) && releasedProbe == null; }

    public List<Integer> buildOrdinals(int stageId) {
        return stageId == plan.buildStageId() ? buildOrdinals : List.of();
    }

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

    /** Releases exactly one scan decision when the build, deadline or query settles first. */
    public CompletionStage<ProbeRelease> releaseProbe(CompletableFuture<?> queryCompletion,
            ScheduledExecutorService deadlines) {
        queryCompletion.whenComplete((result, failure) -> completedBuild.complete(TERMINATED));
        var timeout = completedBuild.isDone() ? null : deadlines.schedule(
                () -> { completedBuild.complete(TIMEOUT); }, MAX_WAIT_MS, TimeUnit.MILLISECONDS);
        return completedBuild.thenApply(decision -> {
            if (timeout != null) timeout.cancel(false);
            releasedProbe = decision;
            return decision;
        });
    }

    public Map<Integer, List<BuildKeyRange>> probeRanges(int stageId) {
        var released = releasedProbe;
        return released != null && isProbe(stageId) ? released.ranges() : null;
    }
}
