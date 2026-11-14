package org.pangrova.planner.lineage;

import org.pangrova.planner.api.lineage.ColumnDerivation;

import org.pangrova.planner.metadata.PlanTable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Reads complete column origins from the query's existing Calcite metadata authority. */
public final class ColumnLineageExtractor {
    /** Extracts read outputs or actual write-target fields, preserving unknown-origin failures. */
    public List<ColumnDerivation> extract(RelNode plan) {
        var query = new QueryColumnOrigins(plan);
        var writes = new ArrayList<TableModify>();
        collectWrites(ColumnLineageKinds.unwrap(plan), writes);
        if (writes.size() > 1) {
            throw new IllegalArgumentException("Column lineage requires one actual write target");
        }
        if (!writes.isEmpty()) {
            return new WriteColumnLineage().extract(writes.get(0), query);
        }
        var fields = plan.getRowType().getFieldList();
        List<ColumnDerivation> result = new ArrayList<>(fields.size());
        for (var field : fields) {
            result.add(field(plan, field.getIndex(), field.getName(), query));
        }
        return List.copyOf(result);
    }

    static ColumnDerivation field(RelNode relation, int output, String name, QueryColumnOrigins query) {
        relation = ColumnLineageKinds.unwrap(relation);
        var origins = PangrovaColumnOriginMetadata.requireOrigins(relation,
                query, output);
        var columns = new LinkedHashSet<ColumnDerivation.SourceColumn>();
        for (var origin : origins) {
            var table = origin.getOriginTable();
            int column = origin.getOriginColumnOrdinal();
            if (table == null || column < 0 || column >= table.getRowType().getFieldCount()
                    || table.getQualifiedName().isEmpty()) {
                throw new IllegalArgumentException("Calcite returned an incomplete column origin");
            }
            columns.add(new ColumnDerivation.SourceColumn(String.join(".", table.getQualifiedName()),
                    table.getRowType().getFieldList().get(column).getName()));
        }
        var ordered = columns.stream().sorted(Comparator.comparing(ColumnDerivation.SourceColumn::table)
                .thenComparing(ColumnDerivation.SourceColumn::column)).toList();
        return new ColumnDerivation(name, ordered, query.classification(relation, output));
    }

    /** Collects source identities directly from the actual compiled table scans. */
    public Set<String> extractSourceTables(RelNode plan) {
        Set<String> tables = new LinkedHashSet<>();
        collectTables(plan, tables);
        return java.util.Collections.unmodifiableSet(tables);
    }

    private void collectTables(RelNode node, Set<String> tables) {
        node = ColumnLineageKinds.unwrap(node);
        if (node instanceof TableScan scan) {
            tables.add(String.join(".", PlanTable.require(scan).getQualifiedName()));
        }
        for (var input : node.getInputs()) {
            collectTables(input, tables);
        }
    }

    private void collectWrites(RelNode node, List<TableModify> writes) {
        node = ColumnLineageKinds.unwrap(node);
        if (node instanceof TableModify modify) {
            writes.add(modify);
            return;
        }
        for (var input : node.getInputs()) {
            collectWrites(input, writes);
        }
    }
}
