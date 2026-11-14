package org.pangrova.planner.analysis;

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import org.pangrova.planner.api.result.ResultColumnModel;

import org.pangrova.common.api.annotation.Internal;

import java.util.List;

/** One opaque stage contract for live query planning and exact durable serialized replay. */
@Internal
public interface StagePlanAccess {

    StagePlanMode mode();

    /**
     * Exact coordinates for durable replay; live plans return empty until fragmented.
     * Every DURABLE_REPLAY implementation must return a value. Callers reject an
     * absent identity rather than treating it as an unfenced live stage.
     */
    java.util.Optional<org.pangrova.planner.api.fragment.StageCoordinates> frozenCoordinates();

    List<SplitSourceDescriptor> splitSources();

    List<String> queryOutputColumns();

    List<String> fieldNames();

    List<ResultColumnModel> resultColumnModels();

    /** Exact declared storage types at their final output ordinals, independent of model bindings. */
    List<org.pangrova.protocol.v2.ResultColumnLogicalType> resultColumnTypes();

    SerializedPlanFragment toSerializedPlan(SqlCompiler compiler, List<String> columnNames);

    /** Explicit live-only capability; serialized replay must reject access instead of rebuilding SQL. */
    PlanFragment requireLiveFragment();
}
