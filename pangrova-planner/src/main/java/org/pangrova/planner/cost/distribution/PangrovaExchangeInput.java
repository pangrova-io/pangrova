package org.pangrova.planner.cost.distribution;

import org.pangrova.planner.api.result.ResultColumnModel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.pangrova.protocol.v2.BatchSerde;

import java.util.List;
import java.util.Objects;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Consumer-side leaf of a fragmented distributed exchange.
 *
 * <p>The producer fragment ends with the official Substrait ExchangeRel. This
 * leaf serializes as Pangrova's typed v2 ExchangeInputDetail and becomes a
 * native Velox ExchangeNode. It never carries endpoints; attempts and Flight
 * endpoints are runtime bindings, keeping the plan deterministic across
 * retries.</p>
 */
public final class PangrovaExchangeInput extends AbstractRelNode {

    private final RelDataType rowType;
    private final Specification specification;
    private final List<ResultColumnModel> resultColumnModels;

    /** Creates a consumer-side exchange leaf from an immutable exchange specification. */
    public PangrovaExchangeInput(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType rowType,
            Specification specification) {
        this(cluster, traitSet, rowType, specification, List.of());
    }

    /** Preserves planner-only VECTOR model provenance across the stage boundary. */
    public PangrovaExchangeInput(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType rowType,
            Specification specification,
            List<ResultColumnModel> resultColumnModels) {
        super(cluster, traitSet);
        this.rowType = Objects.requireNonNull(rowType, "rowType must not be null");
        this.specification = Objects.requireNonNull(
                specification, "specification must not be null");
        this.resultColumnModels = List.copyOf(Objects.requireNonNull(
                resultColumnModels, "resultColumnModels must not be null"));
        for (ResultColumnModel origin : this.resultColumnModels) {
            if (origin.columnIndex() < 0 || origin.columnIndex() >= rowType.getFieldCount()
                    || origin.modelName() == null || origin.modelName().isBlank()) {
                throw new IllegalArgumentException("Invalid exchange VECTOR model origin");
            }
        }
    }

    public String planNodeId() { return specification.planNodeId(); }
    public String exchangeId() { return specification.exchangeId(); }
    public int upstreamStageId() { return specification.upstreamStageId(); }
    public int downstreamStageId() { return specification.downstreamStageId(); }
    public PangrovaDistribution distribution() { return specification.distribution(); }
    public int partitionCount() { return specification.partitionCount(); }
    public BatchSerde batchSerde() { return specification.batchSerde(); }
    public boolean requireComplete() { return specification.requireComplete(); }

    /** Returns the exact model origin for an output column, if one is bound. */
    public String resultModel(int columnIndex) {
        for (ResultColumnModel origin : resultColumnModels) {
            if (origin.columnIndex() == columnIndex) {
                return origin.modelName();
            }
        }
        return null;
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (!inputs.isEmpty()) {
            throw new IllegalArgumentException("PangrovaExchangeInput is a leaf");
        }
        return new PangrovaExchangeInput(
                getCluster(), traitSet, rowType, specification, resultColumnModels);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("exchangeId", exchangeId())
                .item("upstreamStage", upstreamStageId())
                .item("downstreamStage", downstreamStageId())
                .item("distribution", distribution())
                .item("partitions", partitionCount())
                .item("requireComplete", requireComplete());
    }

    /** Immutable protocol-facing attributes of one exchange input. */
    public record Specification(
            String planNodeId,
            String exchangeId,
            int upstreamStageId,
            int downstreamStageId,
            PangrovaDistribution distribution,
            int partitionCount,
            BatchSerde batchSerde,
            boolean requireComplete) {

        /** Validates all attributes when the specification is created. */
        public Specification {
            planNodeId = requireText(planNodeId, "planNodeId");
            exchangeId = requireText(exchangeId, "exchangeId");
            if (upstreamStageId < 0 || downstreamStageId < 0) {
                throw new IllegalArgumentException("stage IDs must be non-negative");
            }
            if (partitionCount < 1) {
                throw new IllegalArgumentException("partitionCount must be >= 1");
            }
            distribution = Objects.requireNonNull(
                    distribution, "distribution must not be null");
            batchSerde = Objects.requireNonNull(batchSerde, "batchSerde must not be null");
            if (batchSerde != BatchSerde.BATCH_SERDE_ARROW_BATCH) {
                throw new IllegalArgumentException("batchSerde must be the canonical columnar batch format");
            }
        }
    }
}
