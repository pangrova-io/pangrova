package org.pangrova.planner.analysis;

import com.google.common.collect.ImmutableList;
import io.substrait.proto.AggregationPhase;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/** A global aggregate phase with an explicit cross-stage state schema. */
public final class StageAggregate extends SingleRel {
    private final AggregationPhase phase;
    private final ImmutableList<AggregateCall> calls;
    private final ImmutableList<AggregateExchangeBinding> bindings;
    private final RelDataType outputType;

    StageAggregate(RelTraitSet traits, RelNode input, List<AggregateCall> calls, RelDataType outputType,
            AggregationPhase phase, List<AggregateExchangeBinding> bindings) {
        super(input.getCluster(), traits, input);
        if (phase != AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE
                && phase != AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT) {
            throw new IllegalArgumentException("Global stage aggregate requires raw or final phase");
        }
        if (calls.isEmpty() || bindings.size() != calls.size() || outputType.getFieldCount() != calls.size()) {
            throw new IllegalArgumentException("Global stage aggregate requires one binding and output per measure");
        }
        this.phase = phase;
        this.calls = ImmutableList.copyOf(calls);
        this.bindings = ImmutableList.copyOf(bindings);
        this.outputType = outputType;
    }

    public AggregationPhase phase() {
        return phase;
    }

    public List<AggregateCall> calls() {
        return calls;
    }

    public List<AggregateExchangeBinding> bindings() {
        return bindings;
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputType;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery metadata) {
        return 1.0;
    }

    @Override
    public StageAggregate copy(RelTraitSet traits, List<RelNode> inputs) {
        return new StageAggregate(traits, sole(inputs), calls, outputType, phase, bindings);
    }

    @Override
    public RelWriter explainTerms(RelWriter writer) {
        return super.explainTerms(writer).item("phase", phase).item("measures", calls);
    }
}
