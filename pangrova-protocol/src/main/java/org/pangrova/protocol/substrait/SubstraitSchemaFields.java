package org.pangrova.protocol.substrait;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.substrait.generated.GeneratedTypeLayoutNaming;
import org.pangrova.protocol.v2.FunctionTypePathStep;

import java.util.ArrayList;
import java.util.List;

/** Preserves the complete DFS name subtree attached to each root field. */
final class SubstraitSchemaFields {
    private static final int MAX_VALUE_TYPE_NODES = 2048;

    record Field(String name, Type type, List<String> descendants) {
        Field { descendants = List.copyOf(descendants); }
    }

    private SubstraitSchemaFields() {}

    static List<Field> fields(NamedStruct schema) {
        if (!schema.hasStruct()) throw new IllegalArgumentException("NamedStruct requires a struct");
        List<Field> result = new ArrayList<>();
        int cursor = 0;
        for (Type type : schema.getStruct().getTypesList()) {
            SubstraitExpressionTypes.declared(type);
            int count = nestedNames(type);
            if (cursor >= schema.getNamesCount() || count > schema.getNamesCount() - cursor - 1) {
                throw new IllegalArgumentException("NamedStruct is missing DFS field names");
            }
            result.add(new Field(schema.getNames(cursor), type,
                    schema.getNamesList().subList(cursor + 1, cursor + count + 1)));
            cursor += count + 1;
        }
        if (cursor != schema.getNamesCount()) {
            throw new IllegalArgumentException("NamedStruct has unused DFS field names");
        }
        return List.copyOf(result);
    }

    static int nestedNames(Type type) {
        if (type.hasStruct()) {
            return type.getStruct().getTypesList().stream()
                    .mapToInt(child -> 1 + nestedNames(child)).sum();
        }
        if (type.hasList()) return nestedNames(type.getList().getType());
        if (type.hasMap()) return nestedNames(type.getMap().getKey()) + nestedNames(type.getMap().getValue());
        return 0;
    }

    static String nameAtPath(Type type, List<String> names, List<Integer> path, int position, int offset) {
        int child = path.get(position);
        if (type.hasStruct()) {
            if (child < 0 || child >= type.getStruct().getTypesCount()) {
                throw new IllegalArgumentException("Named field path has an invalid ROW child");
            }
            int cursor = offset;
            for (int index = 0; index < child; index++) {
                cursor += 1 + nestedNames(type.getStruct().getTypes(index));
            }
            if (position + 1 == path.size()) return names.get(cursor);
            return nameAtPath(type.getStruct().getTypes(child), names, path, position + 1, cursor + 1);
        }
        if (position + 1 == path.size()) {
            throw new IllegalArgumentException("Named field path must terminate at a ROW field");
        }
        if (type.hasList() && child == 0) {
            return nameAtPath(type.getList().getType(), names, path, position + 1, offset);
        }
        if (type.hasMap() && (child == 0 || child == 1)) {
            Type selected = child == 0 ? type.getMap().getKey() : type.getMap().getValue();
            int cursor = offset + (child == 0 ? 0 : nestedNames(type.getMap().getKey()));
            return nameAtPath(selected, names, path, position + 1, cursor);
        }
        throw new IllegalArgumentException("Named field path traverses an invalid type child");
    }

    static List<Integer> childOrdinals(Type type, List<FunctionTypePathStep> path) {
        if (path == null || path.isEmpty() || path.size() > 128 || path.size() % 2 != 0) {
            throw new IllegalArgumentException("Named canonical type path must end at a child Type");
        }
        List<Integer> result = new ArrayList<>();
        Type current = type;
        for (int position = 0; position < path.size(); position += 2) {
            var kind = current.getDescriptorForType().findFieldByNumber(current.getKindCase().getNumber());
            var rootStep = path.get(position);
            if (kind == null || rootStep.getFieldNumber() != kind.getNumber() || rootStep.hasRepeatedIndex()) {
                throw new IllegalArgumentException("Named canonical type path traverses a different kind");
            }
            Message message = (Message) current.getField(kind);
            var childStep = path.get(position + 1);
            int ordinal = childOrdinal(message, childStep);
            var field = message.getDescriptorForType().findFieldByNumber(childStep.getFieldNumber());
            result.add(ordinal);
            current = (Type) (field.isRepeated() ? message.getRepeatedField(field, ordinal) : message.getField(field));
        }
        if (path.stream().anyMatch(step -> !step.getUnknownFields().asMap().isEmpty())) {
            throw new IllegalArgumentException("Named canonical type path has unknown fields");
        }
        return List.copyOf(result);
    }

    private static int childOrdinal(Message message, FunctionTypePathStep childStep) {
        var children = message.getDescriptorForType().getFields().stream()
                .filter(field -> field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                        && field.getMessageType().equals(Type.getDescriptor())).toList();
        var field = message.getDescriptorForType().findFieldByNumber(childStep.getFieldNumber());
        if (!children.contains(field) || field.isRepeated() != childStep.hasRepeatedIndex()) {
            throw new IllegalArgumentException("Named canonical type path is not a declared child Type");
        }
        if (!field.isRepeated() && !message.hasField(field)) {
            throw new IllegalArgumentException("Named canonical type path traverses an absent child");
        }
        int ordinal = field.isRepeated() ? childStep.getRepeatedIndex() : children.indexOf(field);
        if (ordinal < 0 || (field.isRepeated() && ordinal >= message.getRepeatedFieldCount(field))) {
            throw new IllegalArgumentException("Named canonical type path has an out-of-range child");
        }
        return ordinal;
    }

    static NamedStruct schema(List<Field> fields) {
        return schema(fields, Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build());
    }

    static NamedStruct schema(List<Field> fields, Type.Struct metadata) {
        var schema = NamedStruct.newBuilder();
        var struct = metadata.toBuilder().clearTypes();
        for (Field field : fields) {
            schema.addNames(field.name()).addAllNames(field.descendants());
            struct.addTypes(field.type());
        }
        return schema.setStruct(struct).build();
    }

    static Field computed(String name, Type type) {
        SubstraitExpressionTypes.declared(type);
        List<String> names = new ArrayList<>();
        generatedNames(type, names);
        return new Field(name, type, names);
    }

    private static void generatedNames(Type type, List<String> names) {
        if (type.hasStruct()) {
            for (int i = 0; i < type.getStruct().getTypesCount(); i++) {
                names.add(GeneratedTypeLayoutNaming.anonymousComputedField(i));
                generatedNames(type.getStruct().getTypes(i), names);
            }
        } else if (type.hasList()) {
            generatedNames(type.getList().getType(), names);
        } else if (type.hasMap()) {
            generatedNames(type.getMap().getKey(), names);
            generatedNames(type.getMap().getValue(), names);
        }
    }

    static Type withNullability(Type type, Type.Nullability nullability) {
        var kind = type.getDescriptorForType().findFieldByNumber(type.getKindCase().getNumber());
        if (kind == null) throw new IllegalArgumentException("Type has no declared kind");
        Message child = (Message) type.getField(kind);
        FieldDescriptor nullable = child.getDescriptorForType().findFieldByName("nullability");
        if (nullable == null) throw new IllegalArgumentException("Type has no nullability: " + kind.getName());
        return type.toBuilder().setField(kind, child.toBuilder().setField(nullable,
                nullable.getEnumType().findValueByNumber(nullability.getNumber())).build()).build();
    }

    static Type primitive(String kind, Type.Nullability nullability) {
        var builder = Type.newBuilder();
        var field = builder.getDescriptorForType().findFieldByName(kind);
        if (field == null) throw new IllegalArgumentException("Unknown declared type kind: " + kind);
        var value = builder.newBuilderForField(field);
        var nullable = value.getDescriptorForType().findFieldByName("nullability");
        value.setField(nullable, nullable.getEnumType().findValueByNumber(nullability.getNumber()));
        return builder.setField(field, value.build()).build();
    }

    static Type withVariation(Type type, int variation) {
        var kind = type.getDescriptorForType().findFieldByNumber(type.getKindCase().getNumber());
        Message child = (Message) type.getField(kind);
        var field = child.getDescriptorForType().findFieldByName("type_variation_reference");
        if (field == null && variation == 0) return type;
        if (field == null) throw new IllegalArgumentException("Type cannot carry a variation");
        return type.toBuilder().setField(kind, child.toBuilder().setField(field, variation).build()).build();
    }

    static boolean samePhysicalType(Type left, Type right) {
        return withNullability(left, Type.Nullability.NULLABILITY_REQUIRED)
                .equals(withNullability(right, Type.Nullability.NULLABILITY_REQUIRED));
    }

    /** NamedStruct's container is not a value; every actual field and child is. */
    static void requireValueType(Type type) {
        requireValueType(type, 0, new int[] {MAX_VALUE_TYPE_NODES});
    }

    private static void requireValueType(Type type, int depth, int[] remaining) {
        if (depth > 64 || --remaining[0] < 0) {
            throw new IllegalArgumentException("Canonical value type exceeds its validation bound");
        }
        nullable(type);
        if (type.hasStruct()) {
            type.getStruct().getTypesList().forEach(child -> requireValueType(child, depth + 1, remaining));
        } else if (type.hasList()) {
            requireValueType(type.getList().getType(), depth + 1, remaining);
        } else if (type.hasMap()) {
            requireValueType(type.getMap().getKey(), depth + 1, remaining);
            requireValueType(type.getMap().getValue(), depth + 1, remaining);
        }
    }

    static boolean nullable(Type type) {
        var kind = type.getDescriptorForType().findFieldByNumber(type.getKindCase().getNumber());
        if (kind == null) throw new IllegalArgumentException("Canonical value type has no declared kind");
        Message child = (Message) type.getField(kind);
        var field = child.getDescriptorForType().findFieldByName("nullability");
        if (field == null) throw new IllegalArgumentException("Canonical value type has no nullability contract");
        var value = (com.google.protobuf.Descriptors.EnumValueDescriptor) child.getField(field);
        if (value.getNumber() != Type.Nullability.NULLABILITY_REQUIRED.getNumber()
                && value.getNumber() != Type.Nullability.NULLABILITY_NULLABLE.getNumber()) {
            throw new IllegalArgumentException("Canonical value type requires explicit nullability");
        }
        return value.getNumber() == Type.Nullability.NULLABILITY_NULLABLE.getNumber();
    }
}
