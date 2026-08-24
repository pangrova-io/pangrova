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
package org.pangrova.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An opaque fragment of a compiled query plan corresponding to one execution stage.
 * Wraps the Calcite {@link RelNode} sub-tree, keeping Calcite internals within the
 * planner module. Coordinator code holds and passes {@code PlanFragment} without
 * importing any Calcite types.
 *
 * <p>Thread-safe: immutable once constructed.</p>
 */
public final class PlanFragment {

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
     * Coordinator dispatch uses this to force single-task execution for window queries.
     */
    public boolean containsWindowFunction() {
        return hasWindow(planNode);
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

    /**
     * Returns the field names of this fragment's output row type.
     */
    public List<String> fieldNames() {
        return planNode.getRowType().getFieldNames();
    }

    /**
     * Serializes this fragment to Substrait bytes for dispatch to a worker.
     */
    public byte[] toSubstraitBytes(SqlCompiler compiler) {
        return compiler.convertValidateAndSerialize(planNode,
                planNode.getRowType().getFieldNames());
    }

    /**
     * Serializes with explicit column names and optional retry on originalSql.
     */
    public byte[] toSubstraitBytes(SqlCompiler compiler, java.util.List<String> columnNames,
                                    String originalSql) {
        if (originalSql != null && !originalSql.isEmpty()) {
            return compiler.convertValidateAndSerializeWithRetry(planNode, columnNames, originalSql);
        }
        return compiler.convertValidateAndSerialize(planNode, columnNames);
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
            org.pangrova.planner.adaptive.JoinStrategy joinStrategyHint) {
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
