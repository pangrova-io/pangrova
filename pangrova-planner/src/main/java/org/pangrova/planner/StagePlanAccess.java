package org.pangrova.planner;

import org.pangrova.spi.annotation.Internal;

import java.util.List;

/** One opaque stage contract for live query planning and exact durable serialized replay. */
@Internal
public interface StagePlanAccess {
    /** Replay is immutable: no adaptive reoptimization or fresh connector split enumeration. */
    enum Mode { LIVE_QUERY, DURABLE_REPLAY }

    Mode mode();

    List<SplitSourceDescriptor> splitSources();

    List<String> queryOutputColumns();

    List<String> fieldNames();

    List<PlanFragment.ResultColumnModel> resultColumnModels();

    /** Exact declared storage types at their final output ordinals, independent of model bindings. */
    List<org.pangrova.protocol.v2.ResultColumnLogicalType> resultColumnTypes();

    SerializedPlanFragment toSerializedPlan(SqlCompiler compiler, List<String> columnNames);

    /** Explicit live-only capability; serialized replay must reject access instead of rebuilding SQL. */
    PlanFragment requireLiveFragment();
}
