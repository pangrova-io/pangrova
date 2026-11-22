package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;

/** Bounds the delay before the single eligible probe root enumerates its pinned splits. */
public final class BuildFilterGate {
    private static final long MAX_WAIT_MS = 200;

    private final StageRuntimeFilter plan;
    private final CompletableFuture<BuildKeyRange> completedBuild = new CompletableFuture<>();
    private volatile boolean probeReleased;
    private BuildKeyRange releasedRange;

    private BuildFilterGate(StageRuntimeFilter plan) {
        this.plan = plan;
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

    public boolean blocks(int stageId) { return isProbe(stageId) && !probeReleased; }

    public OptionalInt buildOrdinal(int stageId) {
        return stageId == plan.buildStageId() ? OptionalInt.of(plan.buildKeyOrdinal()) : OptionalInt.empty();
    }

    /** Called after the winning cohort has finished and any external publication is fenced. */
    public void onFinished(Stage stage) {
        if (stage.stageId() == plan.buildStageId() && stage.exchangeReadyForConsumers()) {
            completedBuild.complete(stage.completeBuildKeyRange());
        }
    }

    /** Releases exactly one scan decision. A late result cannot affect an issued probe. */
    public void releaseProbe(CompletableFuture<?> queryCompletion) {
        queryCompletion.whenComplete((result, failure) -> completedBuild.complete(null));
        try {
            releasedRange = completedBuild.get(MAX_WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            completedBuild.complete(null);
        } catch (InterruptedException interruption) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting the runtime-filter build", interruption);
        } catch (ExecutionException failure) {
            throw new IllegalStateException("Runtime-filter build observation failed", failure.getCause());
        }
        probeReleased = true;
    }

    public BuildKeyRange probeRange(int stageId) {
        return probeReleased && isProbe(stageId) ? releasedRange : null;
    }
}
