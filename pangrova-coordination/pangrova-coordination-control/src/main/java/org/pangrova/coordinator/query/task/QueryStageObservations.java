package org.pangrova.coordinator.query.task;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.api.stage.StageCompletionCallback.SuccessfulAttempt;
import org.pangrova.query.profiling.OperatorStatsSource.SelectedAttempt;
import org.pangrova.query.profiling.OperatorStatsSource.Exchange;
import org.pangrova.query.profiling.OperatorStatsSource.ExecutionGraph;
import org.pangrova.query.profiling.OperatorStatsSource.Stage;
import org.pangrova.query.profiling.OperatorStatsSource.StageTransition;

/** Records actual stage transitions observed on the owning Coordinator's monotonic clock. */
public final class QueryStageObservations {
    private final long startedAt = System.nanoTime();
    private final List<Stage> stages;
    private final List<Exchange> exchanges;
    private final ConcurrentLinkedQueue<StageTransition> transitions = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<SelectedAttempt> selectedAttempts = new ConcurrentLinkedQueue<>();

    QueryStageObservations(StageDAG dag) {
        stages = dag.stages().stream().map(stage -> new Stage(stage.stageId(), stage.taskType(), stage.parallelism()))
                .toList();
        exchanges = dag.stageEdges().stream().map(edge -> new Exchange(edge.exchangeId(), edge.upstreamStageId(),
                edge.downstreamStageId(), edge.shuffleType().name(), edge.readMode().name(), edge.partitionCount()))
                .toList();
    }

    public void record(int stageId, int generation, StageState previous, StageState state) {
        transitions.add(new StageTransition(stageId, generation, previous.name(), state.name(),
                System.nanoTime() - startedAt));
    }

    public void recordSelection(int stageId, int generation, String logicalTaskId, SuccessfulAttempt attempt) {
        selectedAttempts.add(new SelectedAttempt(stageId, generation, attempt.partitionId(), attempt.planEpoch(),
                attempt.attemptId(), logicalTaskId, attempt.taskId()));
    }

    ExecutionGraph snapshot() {
        var timeline = transitions.stream().sorted(Comparator.comparingLong(StageTransition::elapsedNanos)).toList();
        var selected = selectedAttempts.stream().sorted(Comparator.comparingInt(SelectedAttempt::stageId)
                .thenComparingInt(SelectedAttempt::generation).thenComparingInt(SelectedAttempt::partitionId)
                .thenComparing(SelectedAttempt::taskId)).toList();
        return new ExecutionGraph(stages, exchanges, timeline, selected);
    }
}
