package org.pangrova.coordinator.artifact.template;

import org.pangrova.metadata.artifact.build.ArtifactStageGraph.StagePlan;
import org.pangrova.planner.PlanFragment;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.planner.SplitSourceDescriptor;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.StagePlanAccess;
import org.pangrova.spi.annotation.Internal;

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
    public List<PlanFragment.ResultColumnModel> resultColumnModels() {
        return List.of();
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
