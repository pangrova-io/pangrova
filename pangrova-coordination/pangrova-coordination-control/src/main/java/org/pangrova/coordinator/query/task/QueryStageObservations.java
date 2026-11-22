package org.pangrova.coordinator.query.task;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.query.api.enums.StageState;
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

    ExecutionGraph snapshot() {
        var snapshot = transitions.stream().sorted(Comparator.comparingLong(StageTransition::elapsedNanos)).toList();
        return new ExecutionGraph(stages, exchanges, snapshot);
    }
}
