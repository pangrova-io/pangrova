package org.pangrova.protocol.substrait;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.List;

/** Preserves the complete DFS name subtree attached to each root field. */
final class SubstraitSchemaFields {
    record Field(String name, Type type, List<String> descendants) {
        Field { descendants = List.copyOf(descendants); }
    }

    private SubstraitSchemaFields() {}

    static List<Field> fields(NamedStruct schema) {
        if (!schema.hasStruct()) throw new IllegalArgumentException("NamedStruct requires a struct");
        List<Field> result = new ArrayList<>();
        int cursor = 0;
        for (Type type : schema.getStruct().getTypesList()) {
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
        List<String> names = new ArrayList<>();
        generatedNames(type, names);
        return new Field(name, type, names);
    }

    private static void generatedNames(Type type, List<String> names) {
        if (type.hasStruct()) {
            for (int i = 0; i < type.getStruct().getTypesCount(); i++) {
                names.add(FunctionBindingDeclarations.anonymousComputedField(i));
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
}
