package org.pangrova.planner.core.retrieval;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.protocol.retrieval.RetrievalCandidateIdentity;
import org.pangrova.common.api.annotation.Internal;

/** Adds signed Native virtual identity after SQL validation and policy application. */
@Internal
public final class PhysicalRetrievalIdentityRewriter {
    private PhysicalRetrievalIdentityRewriter() {}

    /** Retains visible ordinal order; only the terminal fragment removes the additions. */
    public static RelNode rewrite(RelNode root) {
        if (!(root instanceof LogicalSort sort) || !supportedInput(sort.getInput())) {
            throw new IllegalArgumentException("Physical retrieval identity requires one proven source chain");
        }
        RelNode input = append(sort.getInput());
        return sort.copy(sort.getTraitSet(), input, sort.getCollation(), sort.offset, sort.fetch);
    }

    /** Checks the actual immutable Calcite structure before selecting the indexed route. */
    public static boolean supportedInput(RelNode input) {
        if (input instanceof LogicalProject project) {
            return project.getRowType().getFieldNames().stream().noneMatch(
                    name -> name.startsWith("$pangrova$")) && supportedInput(project.getInput());
        }
        if (input instanceof LogicalFilter filter) {
            return supportedInput(filter.getInput());
        }
        return input instanceof LogicalTableScan scan
                && scan.getTable() instanceof RelOptTableImpl
                && scan.getRowType().getFieldNames().stream().noneMatch(
                        name -> name.startsWith("$pangrova$")) && physicalSnapshot(scan);
    }

    private static boolean physicalSnapshot(LogicalTableScan scan) {
        var table = scan.getTable();
        if (table == null) {
            return false;
        }
        var binding = table.unwrap(
                org.pangrova.connector.api.metadata.table.ConnectorReadBinding.class);
        if (binding == null || binding.state() != org.pangrova.connector.api.metadata.table.ConnectorReadBinding.SnapshotState.SNAPSHOT || binding.sourceIdentity().isBlank()
                || binding.declaration().declarationEvidence() != org.pangrova.connector.api.metadata.table.ConnectorTableReadMetadata.DeclarationEvidence.EXACT) {
            return false;
        }
        // Paimon publishes this real table option from selected schema.options().
        // A primary-key merge does not expose immutable file-row positions.
        var properties = binding.declaration().metadata().properties();
        return properties.getOrDefault("primary-key", "").isBlank();
    }

    private static RelNode append(RelNode input) {
        if (input instanceof LogicalTableScan scan) {
            if (!(scan.getTable() instanceof RelOptTableImpl table)) {
                throw new IllegalArgumentException("Physical retrieval identity requires a proven table owner");
            }
            var factory = scan.getCluster().getTypeFactory();
            var type = factory.builder().addAll(scan.getRowType().getFieldList());
            for (int index = 0; index < RetrievalCandidateIdentity.PHYSICAL_FIELDS.size(); index++) {
                type.add(RetrievalCandidateIdentity.PHYSICAL_FIELDS.get(index),
                        factory.createTypeWithNullability(factory.createSqlType(
                                index == 3 ? SqlTypeName.BIGINT : SqlTypeName.VARCHAR), false));
            }
            var extendedTable = table.copy(type.build());
            // copy preserves the real table/read-binding owner and original qualified name.
            return LogicalTableScan.create(scan.getCluster(), extendedTable, scan.getHints());
        }
        if (input instanceof LogicalFilter filter) {
            return filter.copy(filter.getTraitSet(), append(filter.getInput()), filter.getCondition());
        }
        LogicalProject project = (LogicalProject) input;
        RelNode extended = append(project.getInput());
        List<RexNode> expressions = new ArrayList<>(project.getProjects());
        List<String> names = new ArrayList<>(project.getRowType().getFieldNames());
        int start = extended.getRowType().getFieldCount() - 4;
        for (int index = 0; index < 4; index++) {
            expressions.add(RexInputRef.of(start + index, extended.getRowType()));
            names.add(RetrievalCandidateIdentity.PHYSICAL_FIELDS.get(index));
        }
        return LogicalProject.create(extended, project.getHints(), expressions, names,
                project.getVariablesSet());
    }
}
