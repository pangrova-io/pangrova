package org.pangrova.planner.lineage;

import org.pangrova.governance.api.enums.ColumnDerivationKind;
import org.pangrova.planner.api.lineage.ColumnDerivation;

import org.pangrova.planner.metadata.PlanTable;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

/** Maps Calcite's actual DML inputs to written columns rather than the ROWCOUNT result. */
final class WriteColumnLineage {
    List<ColumnDerivation> extract(TableModify modify, QueryColumnOrigins query) {
        var target = PlanTable.require(modify);
        var names = target.getRowType().getFieldNames();
        if (modify.isFlattened()) {
            throw new IllegalArgumentException("Flattened DML needs an explicit target-column lineage mapping");
        }
        if (modify.isMerge()) {
            return merge(modify, names, query);
        }
        int width = names.size();
        var updates = modify.getUpdateColumnList();
        int expected = width + (modify.isUpdate() && updates != null ? updates.size() : 0);
        if (modify.getInput().getRowType().getFieldCount() != expected
                || (modify.isUpdate() && (updates == null
                        || updates.size() != new java.util.HashSet<>(updates).size()))) {
            throw new IllegalArgumentException("DML input differs from its actual target/update column layout");
        }
        List<ColumnDerivation> result = new ArrayList<>(width);
        for (int field = 0; field < width; field++) {
            int updated = modify.isUpdate() ? updates.indexOf(names.get(field)) : -1;
            var value = ColumnLineageExtractor.field(modify.getInput(),
                    updated < 0 ? field : width + updated, names.get(field), query);
            if (updated >= 0) {
                // Native copy-on-write retains the old value on rows outside the predicate.
                value = combine(value, ColumnLineageExtractor.field(modify.getInput(), field, names.get(field), query));
            }
            result.add(value);
        }
        return List.copyOf(result);
    }

    private List<ColumnDerivation> merge(TableModify modify, List<String> names, QueryColumnOrigins query) {
        if (!(modify.getInput() instanceof Project project) || !(project.getInput() instanceof Join join)
                || join.getRight().getRowType().getFieldCount() != names.size()) {
            throw new IllegalArgumentException("MERGE lineage requires the actual Project over source/target Join");
        }
        var updates = modify.getUpdateColumnList();
        int updated = updates == null ? 0 : updates.size();
        int width = names.size();
        int outputs = project.getProjects().size();
        boolean insert = join.getJoinType() == JoinRelType.LEFT;
        boolean update = updated > 0;
        if ((join.getJoinType() != JoinRelType.LEFT && join.getJoinType() != JoinRelType.INNER)
                || (update && outputs != (insert ? 2 * width : width) + updated)
                || (updates != null && updated != new java.util.HashSet<>(updates).size())) {
            throw new IllegalArgumentException("MERGE lineage input differs from the supported native write shape");
        }
        List<ColumnDerivation> result = new ArrayList<>(width);
        for (int field = 0; field < width; field++) {
            var old = ColumnLineageExtractor.field(join.getRight(), field, names.get(field), query);
            var value = old;
            if (insert) {
                value = combine(value, ColumnLineageExtractor.field(project, field, names.get(field), query));
            }
            int updateIndex = update ? updates.indexOf(names.get(field)) : -1;
            if (updateIndex >= 0) {
                value = combine(value, ColumnLineageExtractor.field(project,
                        outputs - updated + updateIndex, names.get(field), query));
            }
            result.add(value);
        }
        return List.copyOf(result);
    }

    private static ColumnDerivation combine(ColumnDerivation value, ColumnDerivation preserved) {
        var sources = new LinkedHashSet<>(value.sourceColumns());
        sources.addAll(preserved.sourceColumns());
        var ordered = sources.stream().sorted(Comparator.comparing(ColumnDerivation.SourceColumn::table)
                .thenComparing(ColumnDerivation.SourceColumn::column)).toList();
        var type = ColumnLineageKinds.combine(ColumnDerivationKind.TRANSFORM,
                ColumnLineageKinds.combine(value.expression(), preserved.expression()));
        return new ColumnDerivation(value.outputColumn(), ordered, type);
    }
}
