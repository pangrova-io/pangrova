package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.execution.task.StageDispatchRecord;

import java.util.List;
import java.util.stream.IntStream;

import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.protocol.v2.ExchangeInputBinding;

/** Exposes package-private exchange binding to cross-package property tests. */
public final class ExchangeInputTestAccess {

    private ExchangeInputTestAccess() {}

    public static List<ExchangeInputBinding> build(
            StageNode stage,
            StageDAG dag,
            List<StageDispatchRecord> dispatched,
            int partitionId) {
        return ExchangeSourceBuilder.prepare(stage, dag, dispatched).bind(partitionId, null);
    }

    public static List<ExchangeInputBinding> buildExternalCohort(StageNode stage, StageDAG dag) {
        var prepared = ExchangeSourceBuilder.prepare(stage, dag, List.of());
        var consumer = dag.findStage(stage.stageId());
        int tasks = consumer == null ? stage.parallelism() : consumer.physicalTaskCount();
        return IntStream.range(0, tasks).mapToObj(partition -> prepared.bind(partition, null).get(0)).toList();
    }

    public static void freezeAdaptiveInputs(org.pangrova.coordinator.query.stage.Stage stage, StageDAG dag) {
        var selector = new AdaptiveShuffleInputs(org.pangrova.planner.adaptivequery.AdaptiveQueryConfig.ENABLED);
        stage.freezeShuffleInputs(selector.select(stage, dag));
    }
}
