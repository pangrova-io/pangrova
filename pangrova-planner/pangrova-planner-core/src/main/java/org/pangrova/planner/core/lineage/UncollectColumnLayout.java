package org.pangrova.planner.core.lineage;

import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/** Mirrors Calcite's declared ARRAY/MAP output layout, never an ordinal guess. */
final class UncollectColumnLayout {
    private UncollectColumnLayout() {}

    static List<Integer> inputs(Uncollect uncollect, int output) {
        int fields = uncollect.getRowType().getFieldCount();
        if (output < 0 || output >= fields) {
            throw new IllegalArgumentException("UNNEST lineage output exceeds its declared layout");
        }
        var arguments = uncollect.getInput().getRowType().getFieldList();
        if (uncollect.withOrdinality && output == fields - 1) {
            List<Integer> all = new ArrayList<>(arguments.size());
            for (var argument : arguments) {
                all.add(argument.getIndex());
            }
            return List.copyOf(all);
        }
        int offset = output;
        for (var argument : arguments) {
            var type = argument.getType();
            var component = type.getComponentType();
            int width;
            if (type.getSqlTypeName() == SqlTypeName.MAP) {
                width = 2;
            } else if (component != null) {
                width = component.isStruct() && uncollect.getItemAliases().isEmpty()
                        ? component.getFieldCount() : 1;
            } else {
                throw new IllegalArgumentException("UNNEST lineage requires a resolved collection type");
            }
            if (offset < width) {
                return List.of(argument.getIndex());
            }
            offset -= width;
        }
        throw new IllegalArgumentException("UNNEST lineage layout differs from its declared output");
    }
}
