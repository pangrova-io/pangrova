package org.pangrova.protocol.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.List;

/** Applies the canonical mask to schema/type metadata, retaining DFS names. */
final class SubstraitMasks {
    private record Selected(Type type, List<String> names) {}

    private SubstraitMasks() {}

    static NamedStruct project(NamedStruct schema, Expression.MaskExpression mask) {
        return selectStruct(schema, mask.getSelect(), mask.getMaintainSingularStruct());
    }

    static Type projectType(Type type, Expression.MaskExpression mask) {
        if (!type.hasStruct()) throw new IllegalArgumentException("Root mask requires a struct");
        var field = SubstraitSchemaFields.computed("root", type);
        NamedStruct schema = NamedStruct.newBuilder().addAllNames(field.descendants())
                .setStruct(type.getStruct()).build();
        NamedStruct selected = project(schema, mask);
        if (!mask.getMaintainSingularStruct() && selected.getStruct().getTypesCount() == 1) {
            return selected.getStruct().getTypes(0);
        }
        return Type.newBuilder().setStruct(selected.getStruct()).build();
    }

    private static NamedStruct selectStruct(NamedStruct schema,
            Expression.MaskExpression.StructSelect selection, boolean maintain) {
        var fields = SubstraitSchemaFields.fields(schema);
        List<SubstraitSchemaFields.Field> result = new ArrayList<>();
        for (var item : selection.getStructItemsList()) {
            if (item.getField() < 0 || item.getField() >= fields.size()) {
                throw new IllegalArgumentException("Mask field is out of bounds: " + item.getField());
            }
            var field = fields.get(item.getField());
            if (item.hasChild()) {
                Selected selected = select(field.type(), field.descendants(), item.getChild(), maintain);
                field = new SubstraitSchemaFields.Field(field.name(), selected.type(), selected.names());
            }
            result.add(field);
        }
        NamedStruct selected = SubstraitSchemaFields.schema(result);
        return selected.toBuilder().setStruct(schema.getStruct().toBuilder().clearTypes()
                .addAllTypes(selected.getStruct().getTypesList())).build();
    }

    private static Selected select(Type type, List<String> names,
            Expression.MaskExpression.Select selection, boolean maintain) {
        if (selection.hasStruct()) return struct(type, names, selection.getStruct(), maintain);
        if (selection.hasList()) return list(type, names, selection.getList(), maintain);
        if (selection.hasMap()) return map(type, names, selection.getMap(), maintain);
        throw new IllegalArgumentException("Mask child has no declared selection");
    }

    private static Selected struct(Type type, List<String> names,
            Expression.MaskExpression.StructSelect selection, boolean maintain) {
        if (!type.hasStruct()) throw new IllegalArgumentException("Struct mask requires a struct");
        NamedStruct selected = selectStruct(NamedStruct.newBuilder().addAllNames(names)
                .setStruct(type.getStruct()).build(), selection, maintain);
        if (!maintain && selected.getStruct().getTypesCount() == 1) {
            var field = SubstraitSchemaFields.fields(selected).get(0);
            Type child = field.type();
            if (type.getStruct().getNullability() == Type.Nullability.NULLABILITY_NULLABLE) {
                child = SubstraitSchemaFields.withNullability(child, Type.Nullability.NULLABILITY_NULLABLE);
            }
            return new Selected(child, field.descendants());
        }
        return new Selected(type.toBuilder().setStruct(selected.getStruct()).build(), selected.getNamesList());
    }

    private static Selected list(Type type, List<String> names,
            Expression.MaskExpression.ListSelect selection, boolean maintain) {
        if (!type.hasList()) throw new IllegalArgumentException("List mask requires a list");
        Type element = type.getList().getType();
        Selected child = selection.hasChild() ? select(element, names, selection.getChild(), maintain)
                : new Selected(element, names);
        if (selection.getSelectionCount() == 1 && selection.getSelection(0).hasItem() && !maintain) {
            return new Selected(SubstraitSchemaFields.withNullability(child.type(),
                    Type.Nullability.NULLABILITY_NULLABLE), child.names());
        }
        return new Selected(type.toBuilder().setList(type.getList().toBuilder()
                .setType(child.type())).build(), child.names());
    }

    private static Selected map(Type type, List<String> names,
            Expression.MaskExpression.MapSelect selection, boolean maintain) {
        if (!type.hasMap()) throw new IllegalArgumentException("Map mask requires a map");
        int keyNames = SubstraitSchemaFields.nestedNames(type.getMap().getKey());
        List<String> valueNames = names.subList(keyNames, names.size());
        Selected child = selection.hasChild()
                ? select(type.getMap().getValue(), valueNames, selection.getChild(), maintain)
                : new Selected(type.getMap().getValue(), valueNames);
        List<String> outputNames = new ArrayList<>(names.subList(0, keyNames));
        outputNames.addAll(child.names());
        return new Selected(type.toBuilder().setMap(type.getMap().toBuilder()
                .setValue(child.type())).build(), List.copyOf(outputNames));
    }
}
