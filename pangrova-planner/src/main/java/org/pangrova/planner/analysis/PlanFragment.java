/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner.analysis;

import java.util.Optional;
import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import org.pangrova.planner.api.result.ResultColumnModel;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.type.ModelBoundVectorRelDataType;
import org.pangrova.planner.type.StorageVectorRelDataType;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An opaque fragment of a compiled query plan corresponding to one execution stage.
 * Wraps the Calcite {@link RelNode} sub-tree, keeping Calcite internals within the
 * planner module. Coordinator code holds and passes {@code PlanFragment} without
 * importing any Calcite types.
 *
 * <p>The Calcite tree belongs to one query's planning lifecycle. Planner-only
 * consumers borrow it without concurrent mutation; dispatch serializes it into
 * immutable protocol bytes. This wrapper does not make Calcite nodes thread-safe.</p>
 */
@NotThreadSafe
public final class PlanFragment implements StagePlanAccess {

    private static final Logger LOG = LoggerFactory.getLogger(PlanFragment.class);

    // package-private so PlanFragmenter can access it
    final RelNode planNode;
    private final long estimatedShuffleBytes;
    private final long estimatedShuffleRows;

    public PlanFragment(RelNode planNode, long estimatedShuffleBytes, long estimatedShuffleRows) {
        this.planNode = planNode;
        this.estimatedShuffleBytes = estimatedShuffleBytes;
        this.estimatedShuffleRows = estimatedShuffleRows;
    }

    public long estimatedShuffleBytes() {
        return estimatedShuffleBytes;
    }

    public long estimatedShuffleRows() {
        return estimatedShuffleRows;
    }

    @Override
    public Optional<org.pangrova.planner.api.fragment.StageCoordinates> frozenCoordinates() {
        return Optional.empty();
    }

    @Override
    public StagePlanMode mode() {
        return StagePlanMode.LIVE_QUERY;
    }

    @Override
    public PlanFragment requireLiveFragment() {
        return this;
    }

    /**
     * Returns the internal Calcite RelNode for components that require
     * deep plan traversal (SplitFetcher, IndexAwareSplitFetcher).
     * Coordinator code that should remain Calcite-free must not call this.
     */
    public org.apache.calcite.rel.RelNode relNode() {
        return planNode;
    }

    /**
     * Returns true if this fragment's plan tree contains a Window (analytic function) node.
     * Distribution is enforced by PlanFragmenter before dispatch, not by collapsing scan tasks.
     */
    public boolean containsWindowFunction() {
        return hasWindow(planNode);
    }

    /** Proves source partition coalescing preserves the stage's row or grouped semantics. */
    public boolean supportsCoalescedHashInput(String exchangeId, List<Integer> hashKeys) {
        return PartitionLocalPlanInspector.supportsCoalescedHashInput(planNode, exchangeId, hashKeys);
    }

    private static boolean hasWindow(RelNode node) {
        if (node instanceof Window) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (hasWindow(input)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extracts split source descriptors from this fragment's plan tree.
     * Encapsulates Calcite tree-walking within the planner module.
     */
    public List<SplitSourceDescriptor> splitSources() {
        return PlanSplitSourceExtractor.extract(planNode);
    }

    /**
     * Returns the output column names of this fragment's plan tree.
     */
    public List<String> queryOutputColumns() {
        return PlanSplitSourceExtractor.queryOutputColumns(planNode);
    }

    /** An eligible single-table predicate scope for index selection. */
    public Optional<ScanPredicates> singleScanPredicates() {
        return PlanSplitSourceExtractor.singleScanPredicates(planNode);
    }

    /**
     * Returns the field names of this fragment's output row type.
     */
    public List<String> fieldNames() {
        return planNode.getRowType().getFieldNames();
    }

    /** Returns exact-model intents for SQL-visible VECTOR result columns. */
    public List<ResultColumnModel> resultColumnModels() {
        var fields = planNode.getRowType().getFieldList();
        var results = new java.util.ArrayList<ResultColumnModel>();
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).getType() instanceof ModelBoundVectorRelDataType) {
                String model = resultModel(planNode, index);
                if (model == null) {
                    throw new IllegalStateException(
                            "VECTOR result column has no exact model origin: " + index);
                }
                results.add(new ResultColumnModel(index, model));
            }
        }
        return List.copyOf(results);
    }

    /** Final relational output types carry storage identity through references, aliases and exchanges. */
    @Override
    public List<ResultColumnLogicalType> resultColumnTypes() {
        var fields = planNode.getRowType().getFieldList();
        var results = new java.util.ArrayList<ResultColumnLogicalType>();
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).getType() instanceof StorageVectorRelDataType vector) {
                results.add(ResultColumnLogicalType.newBuilder().setColumnIndex(index)
                        .setLogicalType(vector.logicalType()).build());
            }
        }
        return List.copyOf(results);
    }

    private static String resultModel(RelNode node, int columnIndex) {
        if (node instanceof PangrovaExchangeInput input) {
            return input.resultModel(columnIndex);
        }
        if (node instanceof Project project) {
            var expression = project.getProjects().get(columnIndex);
            if (expression instanceof RexInputRef reference) {
                return resultModel(project.getInput(), reference.getIndex());
            }
            if (expression instanceof RexCall call
                    && "AI_EMBED".equalsIgnoreCase(call.getOperator().getName())
                    && call.getOperands().get(0) instanceof RexLiteral model) {
                return model.getValueAs(String.class);
            }
            return null;
        }
        if (node.getInputs().size() == 1) {
            return resultModel(node.getInput(0), columnIndex);
        }
        return null;
    }

    /**
     * Serializes this fragment to Substrait bytes for dispatch to a worker.
     */
    public byte[] toSubstraitBytes(SqlCompiler compiler) {
        return compiler.convertValidateAndSerialize(planNode,
                planNode.getRowType().getFieldNames());
    }

    /**
     * Serializes the already authorized fragment with explicit column names.
     */
    public byte[] toSubstraitBytes(SqlCompiler compiler, java.util.List<String> columnNames) {
        return toSerializedPlan(compiler, columnNames).planBytes();
    }

    /** Serializes this fragment and retains typed relation intents for protocol binding. */
    public SerializedPlanFragment toSerializedPlan(
            SqlCompiler compiler,
            java.util.List<String> columnNames) {
        return compiler.convertValidateAndSerializeFragment(planNode, columnNames);
    }

    /**
     * Returns a re-optimized copy of this fragment using updated runtime statistics.
     */
    public PlanFragment reoptimize(PangrovaPlanner planner) {
        RelNode reoptimized = planner.reoptimize(planNode);
        return new PlanFragment(reoptimized,
                estimateBytes(reoptimized),
                estimateRows(reoptimized));
    }

    /**
     * Re-optimizes with an explicit join-strategy hint (F-AQE-03).
     */
    public PlanFragment reoptimize(PangrovaPlanner planner,
            org.pangrova.planner.api.enums.JoinStrategy joinStrategyHint) {
        RelNode reoptimized = planner.reoptimize(planNode, joinStrategyHint);
        return new PlanFragment(reoptimized,
                estimateBytes(reoptimized),
                estimateRows(reoptimized));
    }

    static long estimateBytes(RelNode plan) {
        try {
            var mq = plan.getCluster().getMetadataQuery();
            Double rowCount = mq.getRowCount(plan);
            Double avgRowSize = mq.getAverageRowSize(plan);
            if (rowCount != null && avgRowSize != null
                    && !rowCount.isInfinite() && !avgRowSize.isInfinite()) {
                return (long) (rowCount * avgRowSize);
            }
        } catch (Exception ignored) {
            // metadata unavailable — return sentinel
        }
        return -1L;
    }

    static long estimateRows(RelNode plan) {
        try {
            var mq = plan.getCluster().getMetadataQuery();
            Double rowCount = mq.getRowCount(plan);
            if (rowCount != null && !rowCount.isInfinite()) {
                return rowCount.longValue();
            }
        } catch (Exception ignored) {
            // metadata unavailable — return sentinel
        }
        return -1L;
    }
}
