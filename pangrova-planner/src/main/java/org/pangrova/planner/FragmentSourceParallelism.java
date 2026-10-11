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
import org.apache.calcite.rel.core.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Derives fragment parallelism from the relational metadata of its already-bound sources. */
final class FragmentSourceParallelism {
    private static final Logger LOG = LoggerFactory.getLogger(FragmentSourceParallelism.class);

    private FragmentSourceParallelism() {}

    static int resolve(RelNode node, long bytesPerPartition, int maximum, int unknownSourceParallelism) {
        // Calcite's 100-row default is a costing assumption, not observed table cardinality.
        if (hasUnknownSourceCardinality(node)) return Math.min(unknownSourceParallelism, maximum);
        try {
            var metadata = node.getCluster().getMetadataQuery();
            Double rowCount = metadata.getRowCount(node);
            Double rowSize = metadata.getAverageRowSize(node);
            if (isFinitePositive(rowCount) && isFinitePositive(rowSize)) {
                double estimatedBytes = rowCount * rowSize;
                long partitions = (long) Math.ceil(estimatedBytes / bytesPerPartition);
                return (int) Math.max(1, Math.min(maximum, partitions));
            }
        } catch (RuntimeException failure) {
            LOG.warn("event=dynamic_parallelism_unavailable reason={}", failure.getMessage());
        }
        return Math.min(unknownSourceParallelism, maximum);
    }

    private static boolean hasUnknownSourceCardinality(RelNode node) {
        if (node instanceof TableScan scan) {
            var relTable = scan.getTable();
            if (relTable == null) return true;
            var table = relTable.unwrap(org.apache.calcite.schema.Table.class);
            if (table == null) return true;
            var statistic = table.getStatistic();
            if (statistic == null || statistic.getRowCount() == null) return true;
        }
        return node.getInputs().stream().anyMatch(FragmentSourceParallelism::hasUnknownSourceCardinality);
    }

    private static boolean isFinitePositive(Double value) {
        return value != null && value > 0 && Double.isFinite(value);
    }
}
