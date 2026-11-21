package org.pangrova.protocol.substrait;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.SetRel;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.List;

/** Set inputs share exact physical types; the set operation determines nullability. */
final class SubstraitSetShape {
    private SubstraitSetShape() {}

    static NamedStruct describe(SetRel relation, List<NamedStruct> inputs) {
        if (inputs.size() < 2) throw new IllegalArgumentException("Set requires at least two inputs");
        var fields = SubstraitSchemaFields.fields(inputs.get(0));
        if (inputs.stream().anyMatch(input -> SubstraitSchemaFields.fields(input).size() != fields.size())) {
            throw new IllegalArgumentException("Set input arity differs");
        }
        List<SubstraitSchemaFields.Field> result = new ArrayList<>();
        boolean union = relation.getOp() == SetRel.SetOp.SET_OP_UNION_ALL
                || relation.getOp() == SetRel.SetOp.SET_OP_UNION_DISTINCT;
        if (relation.getOp() == SetRel.SetOp.SET_OP_UNSPECIFIED || relation.getOp() == SetRel.SetOp.UNRECOGNIZED) {
            throw new IllegalArgumentException("Set requires a canonical operation");
        }
        for (int i = 0; i < fields.size(); i++) {
            final int ordinal = i;
            var types = inputs.stream().map(input -> fieldType(input, ordinal, fields.size())).toList();
            Type merged = merge(types);
            Type type = union ? merged : outputNullability(types, relation.getOp());
            result.add(new SubstraitSchemaFields.Field(fields.get(i).name(), type, fields.get(i).descendants()));
        }
        return SubstraitSchemaFields.schema(result, inputs.get(0).getStruct());
    }

    private static Type outputNullability(List<Type> types, SetRel.SetOp operation) {
        boolean nullable = SubstraitExpressionTypes.nullable(types.get(0));
        if (operation == SetRel.SetOp.SET_OP_INTERSECTION_PRIMARY) {
            nullable &= types.subList(1, types.size()).stream().anyMatch(SubstraitExpressionTypes::nullable);
        } else if (operation == SetRel.SetOp.SET_OP_INTERSECTION_MULTISET
                || operation == SetRel.SetOp.SET_OP_INTERSECTION_MULTISET_ALL) {
            nullable &= types.stream().allMatch(SubstraitExpressionTypes::nullable);
        }
        return SubstraitSchemaFields.withNullability(types.get(0), nullable
                ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);
    }

    private static Type fieldType(NamedStruct input, int ordinal, int count) {
        var fields = SubstraitSchemaFields.fields(input);
        if (fields.size() != count) throw new IllegalArgumentException("Set input arity differs");
        return fields.get(ordinal).type();
    }

    static Type merge(List<Type> types) {
        if (types.isEmpty()) throw new IllegalArgumentException("Set requires declared input types");
        types.forEach(SubstraitExpressionTypes::declared);
        Type first = types.get(0);
        var kind = first.getKindCase();
        if (types.stream().anyMatch(type -> type.getKindCase() != kind)) {
            throw new IllegalArgumentException("Set input physical types differ");
        }
        if (first.hasStruct()) {
            var builder = first.getStruct().toBuilder().clearTypes();
            int count = first.getStruct().getTypesCount();
            if (types.stream().anyMatch(type -> type.getStruct().getTypesCount() != count)) {
                throw new IllegalArgumentException("Set struct shapes differ");
            }
            for (int i = 0; i < count; i++) {
                final int ordinal = i;
                builder.addTypes(merge(types.stream().map(type -> type.getStruct().getTypes(ordinal)).toList()));
            }
            first = first.toBuilder().setStruct(builder).build();
        } else if (first.hasList()) {
            first = first.toBuilder().setList(first.getList().toBuilder()
                    .setType(merge(types.stream().map(type -> type.getList().getType()).toList()))).build();
        } else if (first.hasMap()) {
            first = first.toBuilder().setMap(first.getMap().toBuilder()
                    .setKey(merge(types.stream().map(type -> type.getMap().getKey()).toList()))
                    .setValue(merge(types.stream().map(type -> type.getMap().getValue()).toList()))).build();
        } else return SubstraitExpressionTypes.homogeneous(types);
        Type merged = first;
        if (types.stream().anyMatch(type -> !SubstraitSchemaFields.samePhysicalType(
                replaceChildren(type, merged), merged))) {
            throw new IllegalArgumentException("Nested physical type parameters differ");
        }
        boolean nullable = types.stream().anyMatch(SubstraitExpressionTypes::nullable);
        return SubstraitSchemaFields.withNullability(first, nullable
                ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);
    }

    private static Type replaceChildren(Type type, Type shape) {
        if (type.hasStruct()) return type.toBuilder().setStruct(type.getStruct().toBuilder().clearTypes()
                .addAllTypes(shape.getStruct().getTypesList())).build();
        if (type.hasList()) return type.toBuilder().setList(type.getList().toBuilder()
                .setType(shape.getList().getType())).build();
        return type.toBuilder().setMap(type.getMap().toBuilder().setKey(shape.getMap().getKey())
                .setValue(shape.getMap().getValue())).build();
    }
}
