package org.pangrova.planner.core.substrait;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/** Flattens Calcite field names using Substrait's depth-first naming rules. */
final class SubstraitFieldNames {

    private SubstraitFieldNames() {
    }

    static List<String> flatten(RelDataType rowType, List<String> topLevelNames) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        boolean useOverrides = topLevelNames.size() == fields.size();
        List<String> names = new ArrayList<>();
        for (int index = 0; index < fields.size(); index++) {
            RelDataTypeField field = fields.get(index);
            String name = useOverrides ? topLevelNames.get(index) : field.getName();
            collect(name, field.getType(), names);
        }
        return List.copyOf(names);
    }

    private static void collect(String name, RelDataType type, List<String> names) {
        names.add(name);
        collectNested(type, names);
    }

    private static void collectNested(RelDataType type, List<String> names) {
        if (type.isStruct()) {
            for (RelDataTypeField nested : type.getFieldList()) {
                collect(nested.getName(), nested.getType(), names);
            }
        } else if (type.getSqlTypeName() == SqlTypeName.ARRAY) {
            collectNested(type.getComponentType(), names);
        } else if (type.getSqlTypeName() == SqlTypeName.MAP) {
            collectNested(type.getKeyType(), names);
            collectNested(type.getValueType(), names);
        }
    }
}
