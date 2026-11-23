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

import org.pangrova.planner.security.expression.MaskExpressionParser;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.mask.TableColumnMasks;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Applies column masking policies to a RelNode plan.
 * For each TableScan, loads mask policies from {@link AccessControl} per column/user
 * and wraps the scan with a {@link LogicalProject} that replaces masked columns
 * with literal mask expressions.
 *
 * <p>Thread-safe: stateless beyond the injected {@code AccessControl} reference.
 */
public final class ColumnMaskApplier {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnMaskApplier.class);

    private final AccessControl accessControl;
    private final PolicyObservationSink observations;
    private final java.util.Map<QualifiedTableName, QualifiedTableName> physicalToLogical;

    public ColumnMaskApplier(AccessControl accessControl) {
        this(accessControl, null, java.util.Map.of());
    }

    private ColumnMaskApplier(AccessControl accessControl, PolicyObservationSink observations,
            java.util.Map<QualifiedTableName, QualifiedTableName> physicalToLogical) {
        this.accessControl = java.util.Objects.requireNonNull(accessControl, "accessControl");
        this.observations = observations;
        this.physicalToLogical = java.util.Map.copyOf(physicalToLogical);
    }

    /** Binds observations to this query without changing the policy authority. */
    public ColumnMaskApplier observing(PolicyObservationSink sink) {
        return new ColumnMaskApplier(accessControl, java.util.Objects.requireNonNull(sink, "sink"),
                physicalToLogical);
    }

    /** Verifies the policy provider against the authority that admitted the query. */
    public boolean usesAuthority(AccessControl authority) {
        return accessControl == authority;
    }

    /** Uses logical Dynamic Table policy identity for this query's physical scans. */
    public ColumnMaskApplier withAuthorityMapping(
            java.util.Map<QualifiedTableName, QualifiedTableName> mapping) {
        return new ColumnMaskApplier(accessControl, observations, mapping);
    }

    /**
     * Applies column masks to a raw RelNode plan. No-op when {@code identity} is null.
     *
     * @return the transformed plan with masked columns, or the original if no masks apply
     */
    public RelNode applyColumnMasks(RelNode relNode, UserIdentity identity) {
        if (identity == null) {
            return relNode;
        }
        return applyAuthorizedMasks(relNode, accessControl.beginReadAuthorization(identity));
    }

    public RelNode applyAuthorizedMasks(RelNode relNode, AccessControl.ReadAuthorization authorization) {
        return applyMasksRecursive(relNode, authorization, new java.util.HashMap<>());
    }

    private RelNode applyMasksRecursive(RelNode node, AccessControl.ReadAuthorization authorization,
            Map<QualifiedTableName, TableColumnMasks> tableMasks) {
        if (node instanceof org.apache.calcite.rel.logical.LogicalTableModify modify
                && MutationPolicyInputs.modifiesExistingRows(modify)) {
            return MutationPolicyInputs.columnMasks(modify,
                    input -> applyMasksRecursive(input, authorization, tableMasks),
                    scan -> applyMasksToScan(scan, authorization, tableMasks));
        }
        if (node instanceof TableScan scan) {
            return applyMasksToScan(scan, authorization, tableMasks);
        }
        return PlanNodeTransform.replaceInputs(node,
                input -> applyMasksRecursive(input, authorization, tableMasks));
    }

    private RelNode applyMasksToScan(TableScan scan, AccessControl.ReadAuthorization authorization,
            Map<QualifiedTableName, TableColumnMasks> tableMasks) {
        List<RelDataTypeField> fields = scan.getRowType().getFieldList();
        if (fields.isEmpty()) return scan;
        QualifiedTableName ref = PlanNodeTransform.extractTableRef(scan);
        QualifiedTableName authority = policyTable(ref);
        var masks = tableMasks.computeIfAbsent(authority, table -> authorization.columnMasks(
                table.catalog(), table.schema(), table.table()));
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

        List<RexNode> projects = new ArrayList<>();
        List<String> names = new ArrayList<>();
        boolean hasMask = false;

        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            hasMask |= collectMaskedProjection(
                    ref, masks, rexBuilder, scan, field, i, projects);
            names.add(field.getName());
        }
        return hasMask
                ? LogicalProject.create(scan, List.of(), projects, names)
                : scan;
    }

    private boolean collectMaskedProjection(QualifiedTableName ref, TableColumnMasks masks,
                                            RexBuilder rexBuilder, TableScan scan,
                                            RelDataTypeField field, int index,
                                            List<RexNode> projects) {
        Optional<String> mask = masks.mask(field.getName());
        if (observations != null) {
            observations.columnMask(ref, field.getName(), mask);
        }
        if (mask.isPresent()) {
            RexNode maskNode = MaskExpressionParser.parse(mask.get(), scan);
            RexNode castNode = ensureTypeCompatible(rexBuilder, maskNode, field.getType());
            projects.add(castNode);
            LOG.debug("Column mask applied: table={}.{}.{}, column={}",
                    ref.catalog(), ref.schema(), ref.table(), field.getName());
            return true;
        }
        projects.add(rexBuilder.makeInputRef(field.getType(), index));
        return false;
    }

    private QualifiedTableName policyTable(QualifiedTableName physical) {
        return physicalToLogical.getOrDefault(physical, physical);
    }

    private static RexNode ensureTypeCompatible(RexBuilder rexBuilder, RexNode maskNode,
                                                 org.apache.calcite.rel.type.RelDataType targetType) {
        if (maskNode.getType().equals(targetType)) {
            return maskNode;
        }
        return rexBuilder.makeCast(targetType, maskNode, true);
    }
}
