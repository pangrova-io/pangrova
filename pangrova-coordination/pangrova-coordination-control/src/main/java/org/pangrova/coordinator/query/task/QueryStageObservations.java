package org.pangrova.coordinator.query.task;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.stage.StageCompletionCallback.SuccessfulAttempt;
import org.pangrova.query.profiling.enums.DispatchPhase;
import org.pangrova.query.profiling.OperatorStatsSource.DownstreamPreparation;
import org.pangrova.query.profiling.OperatorStatsSource.Exchange;
import org.pangrova.query.profiling.OperatorStatsSource.ExecutionGraph;
import org.pangrova.query.profiling.enums.PreparationOutcome;
import org.pangrova.query.profiling.enums.RuntimeFilterOutcome;
import org.pangrova.query.profiling.OperatorStatsSource.RuntimeFilterWait;
import org.pangrova.query.profiling.OperatorStatsSource.SelectedAttempt;
import org.pangrova.query.profiling.OperatorStatsSource.Stage;
import org.pangrova.query.profiling.OperatorStatsSource.StageDispatchWork;
import org.pangrova.query.profiling.OperatorStatsSource.StageTransition;

/** Records actual stage transitions and scheduling work on the owning Coordinator's monotonic clock. */
public final class QueryStageObservations {
    private final long startedAt = System.nanoTime();
    private final QueryStageMetrics metrics;
    private final List<Stage> stages;
    private final List<Exchange> exchanges;
    private final ConcurrentLinkedQueue<StageTransition> transitions = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<SelectedAttempt> selectedAttempts = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Preparation> preparations = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<StageDispatchWork> stageDispatchWork = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<RuntimeFilterWait> runtimeFilterWaits;

    QueryStageObservations(StageDAG dag, QueryStageMetrics metrics) {
        this.metrics = metrics;
        runtimeFilterWaits = dag.runtimeFilters().isEmpty() ? null : new ConcurrentLinkedQueue<>();
        stages = dag.stages().stream().map(stage -> new Stage(stage.stageId(), stage.taskType(), stage.parallelism()))
                .toList();
        exchanges = dag.stageEdges().stream().map(edge -> new Exchange(edge.exchangeId(), edge.upstreamStageId(),
                edge.downstreamStageId(), edge.shuffleType().name(), edge.readMode().name(), edge.partitionCount()))
                .toList();
    }

    public void record(int stageId, int generation, StageState previous, StageState state) {
        transitions.add(new StageTransition(stageId, generation, previous.name(), state.name(),
                elapsedNanos()));
    }

    public void recordSelection(int stageId, int generation, String logicalTaskId, SuccessfulAttempt attempt) {
        selectedAttempts.add(new SelectedAttempt(stageId, generation, attempt.partitionId(), attempt.planEpoch(),
                attempt.attemptId(), logicalTaskId, attempt.taskId()));
    }

    /** Records submission before the work enters the executor. */
    public Preparation submitPreparation() {
        var preparation = new Preparation(elapsedNanos());
        preparations.add(preparation);
        return preparation;
    }

    /** Returns a timestamp relative to the enclosing physical graph. */
    public long elapsedNanos() {
        return System.nanoTime() - startedAt;
    }

    /** Records a measured interval for one actual Stage generation. */
    public void recordDispatchWork(int stageId, int generation, DispatchPhase phase, long start,
            Integer itemCount, boolean successful) {
        var work = new StageDispatchWork(stageId, generation, phase, start,
                elapsedNanos(), itemCount, successful);
        stageDispatchWork.add(work);
        if (metrics != null) metrics.recordDispatch(work);
    }

    /** Records the actual decision before the held probe enumerates files. */
    public void recordRuntimeFilterWait(int buildStageId, int probeStageId, int probeGeneration,
            long startedNanos, RuntimeFilterOutcome outcome, int rangeCount) {
        runtimeFilterWaits.add(new RuntimeFilterWait(buildStageId, probeStageId, probeGeneration,
                startedNanos, elapsedNanos(), outcome, rangeCount));
    }

    /** One scheduler-owned operation, with a consistent snapshot at each transition. */
    public final class Preparation {
        private volatile DownstreamPreparation latest;

        private Preparation(long submittedNanos) {
            latest = new DownstreamPreparation(submittedNanos, null, null, PreparationOutcome.QUEUED);
        }

        public void start() {
            latest = new DownstreamPreparation(latest.submittedNanos(), elapsedNanos(), null,
                    PreparationOutcome.RUNNING);
        }

        public void finish(PreparationOutcome outcome) {
            latest = new DownstreamPreparation(latest.submittedNanos(), latest.startedNanos(), elapsedNanos(), outcome);
            if (metrics != null) metrics.recordPreparation(latest);
        }
    }

    ExecutionGraph snapshot() {
        var timeline = transitions.stream().sorted(Comparator.comparingLong(StageTransition::elapsedNanos)).toList();
        var selected = selectedAttempts.stream().sorted(Comparator.comparingInt(SelectedAttempt::stageId)
                .thenComparingInt(SelectedAttempt::generation).thenComparingInt(SelectedAttempt::partitionId)
                .thenComparing(SelectedAttempt::taskId)).toList();
        var preparation = preparations.stream().map(work -> work.latest)
                .sorted(Comparator.comparingLong(DownstreamPreparation::submittedNanos)).toList();
        var dispatch = stageDispatchWork.stream().sorted(Comparator.comparingLong(StageDispatchWork::startedNanos))
                .toList();
        List<RuntimeFilterWait> filterWaits = runtimeFilterWaits == null ? List.of() : runtimeFilterWaits.stream()
                .sorted(Comparator.comparingInt(RuntimeFilterWait::probeStageId)).toList();
        return new ExecutionGraph(stages, exchanges, timeline, selected, preparation, dispatch, filterWaits);
    }
}
