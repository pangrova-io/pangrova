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
package org.pangrova.planner.security;

import org.pangrova.planner.PlanTable;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Shared utilities for recursive RelNode tree transformations used by
 * security policy appliers (column masking, row filtering).
 *
 * <p>Package-private — not part of the public API.
 */
final class PlanNodeTransform {

    private PlanNodeTransform() {}

    /**
     * Recursively replaces inputs of a RelNode using the given transform function.
     * Returns the original node if no inputs changed (structural sharing).
     */
    static RelNode replaceInputs(RelNode node, UnaryOperator<RelNode> transform) {
        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode transformed = transform.apply(input);
            newInputs.add(transformed);
            if (transformed != input) {
                changed = true;
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    /**
     * Extracts a {@link QualifiedTableName} from a Calcite {@link TableScan} node.
     */
    static QualifiedTableName extractTableRef(TableScan scan) {
        List<String> qn = PlanTable.require(scan).getQualifiedName();
        return QualifiedTableName.resolve(qn);
    }
}
