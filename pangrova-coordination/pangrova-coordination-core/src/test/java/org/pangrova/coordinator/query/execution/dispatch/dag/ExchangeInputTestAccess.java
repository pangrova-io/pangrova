package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.query.core.task.StageDispatchRecord;

import java.util.List;

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
        return ExchangeSourceBuilder.buildExchangeInputs(
                stage, dag, dispatched, partitionId);
    }
}
