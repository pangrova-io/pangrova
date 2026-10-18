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

import org.pangrova.planner.security.expression.FilterExpressionParser;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AuthenticationRequiredException;
import org.pangrova.spi.security.UserIdentity;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Applies row-level security filters to a RelNode plan.
 * For each TableScan, loads RLS policies from {@link AccessControl} per table/user
 * and wraps the scan with a {@link LogicalFilter} containing the policy predicate.
 *
 * <p>Thread-safe: stateless beyond the injected references.
 */
public final class RowFilterApplier {

    private static final Logger LOG = LoggerFactory.getLogger(RowFilterApplier.class);

    private final AccessControl accessControl;
    private final PolicyObservationSink observations;
    private final java.util.Map<QualifiedTableName, QualifiedTableName> physicalToLogical;

    public RowFilterApplier(AccessControl accessControl) {
        this(accessControl, null, java.util.Map.of());
    }

    private RowFilterApplier(AccessControl accessControl, PolicyObservationSink observations,
            java.util.Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        this.accessControl = java.util.Objects.requireNonNull(accessControl, "accessControl");
        this.observations = observations;
        this.physicalToLogical = java.util.Map.copyOf(physicalToLogical);
    }

    /** Binds observations to this query without changing the policy authority. */
    public RowFilterApplier observing(PolicyObservationSink sink) {
        return new RowFilterApplier(accessControl, java.util.Objects.requireNonNull(sink, "sink"),
                physicalToLogical);
    }

    /** Uses logical Dynamic Table policy identity for this query's physical scans. */
    public RowFilterApplier withAuthorityMapping(
            java.util.Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new RowFilterApplier(accessControl, observations, mapping);
    }

    /**
     * Applies row filters to the plan.
     *
     * @throws AuthenticationRequiredException if identity is null (anonymous access)
     * @return the transformed plan with row filters, or the original if no filters apply
     */
    public RelNode applyRowFilters(RelNode relNode, UserIdentity identity) {
        if (identity == null) {
            throw new AuthenticationRequiredException(
                    "Row-level security requires an authenticated identity");
        }
        return applyFiltersRecursive(relNode, identity);
    }

    private RelNode applyFiltersRecursive(RelNode node, UserIdentity identity) {
        if (node instanceof org.apache.calcite.rel.logical.LogicalTableModify modify
                && MutationPolicyInputs.modifiesExistingRows(modify)) {
            return MutationPolicyInputs.rowFilters(modify,
                    input -> applyFiltersRecursive(input, identity),
                    scan -> applyFilterToScan(scan, identity));
        }
        if (node instanceof TableScan scan) {
            return applyFilterToScan(scan, identity);
        }
        return PlanNodeTransform.replaceInputs(node,
                input -> applyFiltersRecursive(input, identity));
    }

    private RelNode applyFilterToScan(TableScan scan, UserIdentity identity) {
        QualifiedTableName ref = PlanNodeTransform.extractTableRef(scan);
        QualifiedTableName authority = physicalToLogical.getOrDefault(ref, ref);
        Optional<String> filter = accessControl.getRowFilter(
                identity, authority.catalog(), authority.schema(), authority.table());
        if (observations != null) {
            observations.rowFilter(ref, filter);
        }
        if (filter.isEmpty()) {
            return scan;
        }
        String filterExpr = filter.get();
        RelDataType fullTableRowType = PlanTable.require(scan).getRowType();
        RelDataType scanRowType = scan.getRowType();
        RexNode condition = FilterExpressionParser.parse(
                filterExpr, fullTableRowType, scan.getCluster());
        RexNode remappedCondition = remapColumnIndices(
                condition, fullTableRowType, scanRowType);
        LOG.debug("Row filter applied: table={}.{}.{}, filter={}",
                ref.catalog(), ref.schema(), ref.table(), filterExpr);
        return LogicalFilter.create(scan, remappedCondition);
    }

    /**
     * Remaps RexInputRef indices from the full table schema to the scan's current RowType.
     */
    private RexNode remapColumnIndices(RexNode condition, RelDataType fullRowType,
                                       RelDataType scanRowType) {
        List<RelDataTypeField> fullFields = fullRowType.getFieldList();
        List<RelDataTypeField> scanFields = scanRowType.getFieldList();
        if (fullFields.size() == scanFields.size() && fullFields.equals(scanFields)) {
            return condition;
        }
        return condition.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                int originalIndex = inputRef.getIndex();
                if (originalIndex >= fullFields.size()) {
                    return inputRef;
                }
                String columnName = fullFields.get(originalIndex).getName();
                int newIndex = findFieldIndex(scanFields, columnName);
                if (newIndex < 0) {
                    throw new IllegalStateException(
                            "Row filter references column '" + columnName
                                    + "' which is not present in the scan's RowType.");
                }
                if (newIndex == originalIndex) {
                    return inputRef;
                }
                return new RexInputRef(newIndex, inputRef.getType());
            }
        });
    }

    private static int findFieldIndex(List<RelDataTypeField> fields, String columnName) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }
}
