package org.pangrova.coordinator.generation.template;
import org.pangrova.planner.api.fragment.StageCoordinates;

import org.pangrova.generation.execution.template.FrozenStageBindings;

import org.pangrova.planner.api.result.ResultColumnModel;

import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;
import org.pangrova.planner.analysis.SplitSourceDescriptor;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.planner.analysis.StagePlanAccess;
import org.pangrova.common.api.annotation.Internal;

import java.util.List;
import java.util.Objects;

/** Opaque persisted stage access for the ordinary DAG; source allocations live in exact durable pages. */
@Internal
public final class PersistedStagePlan implements StagePlanAccess {
    private final StagePlan stage;

    /** Uses the canonical graph's executable stage, without a planner node or reconstructed SQL. */
    public PersistedStagePlan(StagePlan stage) {
        this.stage = Objects.requireNonNull(stage, "stage");
    }

    @Override
    public java.util.Optional<org.pangrova.planner.api.fragment.StageCoordinates> frozenCoordinates() {
        return java.util.Optional.of(new StageCoordinates(stage.stageId(), stage.parallelism(), taskType()));
    }

    @Override
    public Mode mode() {
        return Mode.DURABLE_REPLAY;
    }

    @Override
    public List<SplitSourceDescriptor> splitSources() {
        return List.of();
    }

    @Override
    public List<String> queryOutputColumns() {
        return stage.outputNames();
    }

    @Override
    public List<String> fieldNames() {
        return stage.outputNames();
    }

    @Override
    public List<ResultColumnModel> resultColumnModels() {
        return List.of();
    }

    @Override
    public List<org.pangrova.protocol.v2.ResultColumnLogicalType> resultColumnTypes() {
        return FrozenStageBindings.resultColumnTypes(stage);
    }

    @Override
    public SerializedPlanFragment toSerializedPlan(SqlCompiler compiler, List<String> names) {
        if (!stage.outputNames().equals(names)) {
            throw new IllegalArgumentException("Persisted stage output ordinals cannot be rebound");
        }
        return new SerializedPlanFragment(stage.planBytes(), List.of(), List.of());
    }

    @Override
    public PlanFragment requireLiveFragment() {
        throw new IllegalStateException("Persisted stage has no live compilation capability");
    }

    /** Exact metadata contract, including the immutable partition cohort and frozen bindings. */
    public StagePlan stage() {
        return stage;
    }

    /** Exact task purpose is part of the persisted stage, not a live caller's reinterpretation. */
    public String taskType() {
        return switch (stage.pageKind()) {
            case RELATIONAL_COMPUTE -> "COMPUTE";
            case RELATIONAL_PARTITION -> "OUTPUT";
            case SOURCE_MAP, DEPENDENCY_REDUCE, GENERATION_FINALIZE -> "BACKGROUND_BUILD";
        };
    }
}
