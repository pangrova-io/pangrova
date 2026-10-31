package org.pangrova.protocol.type;

import io.substrait.proto.Type;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.TypeConstraint;
import org.pangrova.connector.spi.data.type.TypeSignature;
import org.pangrova.connector.spi.data.type.DeclaredType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Projects complete connector declarations for the supported physical-write type domain.
 *
 * <p>The canonical logical contract is shared across runtimes. Type-domain admission
 * remains explicit; this projector does not infer missing declarations or advertise
 * support for every connector read type.</p>
 */
public final class DeclaredLogicalTypes {
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final Map<String, String> PRIMITIVES = Map.ofEntries(
            Map.entry("BOOLEAN", "bool"), Map.entry("TINYINT", "i8"), Map.entry("SMALLINT", "i16"),
            Map.entry("INTEGER", "i32"), Map.entry("BIGINT", "i64"), Map.entry("REAL", "fp32"),
            Map.entry("FLOAT", "fp32"), Map.entry("DOUBLE", "fp64"), Map.entry("DATE", "date"),
            Map.entry("TIME", "time"), Map.entry("VARBINARY", "binary"), Map.entry("UUID", "uuid"));

    private DeclaredLogicalTypes() {}

    /**
     * Projects the exact ordered declaration into the canonical logical contract.
     *
     * @param declared complete connector declaration, including recursive nullability
     * @return canonical type and its structural constraints
     * @throws IllegalArgumentException if the declaration is unsupported, incomplete or inconsistent
     */
    public static LogicalType logicalType(DeclaredType declared) {
        validate(declared, 0);
        if ("VECTOR".equals(declared.signature().base())) {
            return storageVector(declared);
        }
        var constraints = new ArrayList<TypeConstraint>();
        nestedConstraints(declared, "", constraints);
        return LogicalType.newBuilder().setCanonicalSql(canonicalSql(declared))
                .setSubstraitType(physicalType(declared, 0)).addAllConstraints(constraints).build();
    }

    private static void validate(DeclaredType type, int depth) {
        if (depth > 64) throw new IllegalArgumentException("Physical type nesting exceeds 64 levels");
        var signature = type.signature();
        String base = signature.base();
        if ("VECTOR".equals(base)) {
            validateVector(type);
        } else if (List.of("ARRAY", "MAP", "ROW").contains(base)) {
            validateComposite(type);
        } else {
            if (!type.children().isEmpty()) throw malformed(signature);
            validateScalar(signature);
        }
        type.children().forEach(child -> validate(child, depth + 1));
    }

    private static void validateVector(DeclaredType type) {
        var signature = type.signature();
        if (signature.parameters().size() != 2 || type.children().size() != 1) throw malformed(signature);
        var dimension = signature.parameters().get(0);
        var element = signature.parameters().get(1);
        if (dimension.longValue() == null || dimension.typeSignature() != null || dimension.fieldName() != null
                || element.longValue() != null || element.fieldName() != null || element.typeSignature() == null) {
            throw malformed(signature);
        }
        var child = type.children().get(0).signature();
        var expected = element.typeSignature();
        if (!expected.equals(child) && !(expected.equals(new TypeSignature("FLOAT"))
                && child.equals(new TypeSignature("REAL")))) {
            throw malformed(signature);
        }
        storageVector(type);
    }

    private static void validateComposite(DeclaredType type) {
        var signature = type.signature();
        String base = signature.base();
        int count = "ARRAY".equals(base) ? 1 : "MAP".equals(base) ? 2 : signature.parameters().size();
        if (count == 0 || signature.parameters().size() != count || type.children().size() != count) {
            throw malformed(signature);
        }
        for (int index = 0; index < count; index++) {
            var parameter = signature.parameters().get(index);
            if (parameter.longValue() != null || parameter.typeSignature() == null
                    || !parameter.typeSignature().equals(type.children().get(index).signature())) {
                throw malformed(signature);
            }
            if ("ROW".equals(base) ? parameter.fieldName() == null || parameter.fieldName().isBlank()
                    : parameter.fieldName() != null) {
                throw malformed(signature);
            }
        }
        if ("ROW".equals(base) && signature.parameters().stream().map(TypeSignature.TypeParameter::fieldName)
                .distinct().count() != count) {
            throw malformed(signature);
        }
    }

    private static void validateScalar(TypeSignature signature) {
        if (PRIMITIVES.containsKey(signature.base())) {
            if (!signature.parameters().isEmpty()) throw malformed(signature);
            return;
        }
        int count = "DECIMAL".equals(signature.base()) ? 2 : 1;
        if (signature.parameters().size() != count || signature.parameters().stream().anyMatch(parameter ->
                parameter.longValue() == null || parameter.typeSignature() != null || parameter.fieldName() != null)) {
            throw malformed(signature);
        }
        int first = parameter(signature, 0);
        boolean valid = switch (signature.base()) {
            case "VARCHAR", "CHAR" -> first > 0;
            case "TIMESTAMP", "TIMESTAMP_TZ" -> first >= 0 && first <= 9;
            case "DECIMAL" -> first > 0 && first <= MAX_DECIMAL_PRECISION && parameter(signature, 1) >= 0
                    && parameter(signature, 1) <= first;
            default -> false;
        };
        if (!valid) throw malformed(signature);
    }

    private static LogicalType storageVector(DeclaredType type) {
        String element = type.signature().parameters().get(1).typeSignature().base();
        return VectorLogicalType.create(parameter(type.signature(), 0), switch (element) {
            case "FLOAT", "REAL" -> VectorLogicalType.FLOAT32;
            case "DOUBLE" -> VectorLogicalType.FLOAT64;
            default -> throw malformed(type.signature());
        }, type.nullable(), type.children().get(0).nullable());
    }

    /** Index paths are structural, independent of user field names or escaping. */
    private static void nestedConstraints(DeclaredType type, String path, List<TypeConstraint> constraints) {
        if ("VECTOR".equals(type.signature().base())) {
            constraints.add(TypeConstraint.newBuilder().setName("dimension@" + path)
                    .setIntegerValue(parameter(type.signature(), 0)).build());
            constraints.add(TypeConstraint.newBuilder().setName("element_type@" + path)
                    .setStringValue("DOUBLE".equals(type.children().get(0).signature().base())
                            ? VectorLogicalType.FLOAT64 : VectorLogicalType.FLOAT32).build());
        }
        for (int index = 0; index < type.children().size(); index++) {
            String childPath = path.isEmpty() ? Integer.toString(index) : path + "." + index;
            if ("ROW".equals(type.signature().base())) {
                constraints.add(TypeConstraint.newBuilder().setName("field_name@" + childPath)
                        .setStringValue(type.signature().parameters().get(index).fieldName()).build());
            }
            nestedConstraints(type.children().get(index), childPath, constraints);
        }
    }

    private static Type physicalType(DeclaredType declaration, int depth) {
        if (depth > 64) throw new IllegalArgumentException("Physical type nesting exceeds 64 levels");
        var signature = declaration.signature();
        var nullability = declaration.nullable() ? Type.Nullability.NULLABILITY_NULLABLE
                : Type.Nullability.NULLABILITY_REQUIRED;
        var children = declaration.children();
        String base = signature.base();
        if ("ARRAY".equals(base) || "VECTOR".equals(base)) {
            requireChildren(declaration, 1);
            return Type.newBuilder().setList(Type.List.newBuilder().setNullability(nullability)
                    .setType(physicalType(children.get(0), depth + 1))).build();
        }
        if ("MAP".equals(base)) {
            requireChildren(declaration, 2);
            return Type.newBuilder().setMap(Type.Map.newBuilder().setNullability(nullability)
                    .setKey(physicalType(children.get(0), depth + 1))
                    .setValue(physicalType(children.get(1), depth + 1))).build();
        }
        if ("ROW".equals(base)) {
            requireChildren(declaration, signature.parameters().size());
            return Type.newBuilder().setStruct(Type.Struct.newBuilder().setNullability(nullability)
                    .addAllTypes(children.stream().map(child -> physicalType(child, depth + 1)).toList())).build();
        }
        if (!children.isEmpty()) throw malformed(signature);
        return scalar(signature, nullability);
    }

    private static Type scalar(TypeSignature signature, Type.Nullability nullability) {
        var primitive = PRIMITIVES.get(signature.base());
        if (primitive != null) return primitive(primitive, nullability, Map.of());
        return switch (signature.base()) {
            case "VARCHAR" -> parameter(signature, 0) == Integer.MAX_VALUE
                    ? primitive("string", nullability, Map.of())
                    : primitive("varchar", nullability, Map.of("length", parameter(signature, 0)));
            case "CHAR" -> primitive("fixed_char", nullability, Map.of("length", parameter(signature, 0)));
            case "DECIMAL" -> primitive("decimal", nullability,
                    Map.of("precision", parameter(signature, 0), "scale", parameter(signature, 1)));
            case "TIMESTAMP", "TIMESTAMP_TZ" -> primitive("TIMESTAMP".equals(signature.base())
                    ? "precision_timestamp" : "precision_timestamp_tz", nullability,
                    Map.of("precision", parameter(signature, 0)));
            default -> throw malformed(signature);
        };
    }

    private static Type primitive(String kind, Type.Nullability nullable, Map<String, Integer> parameters) {
        var builder = Type.newBuilder();
        var field = builder.getDescriptorForType().findFieldByName(kind);
        if (field == null) throw new IllegalArgumentException("Unknown physical type kind: " + kind);
        var value = builder.newBuilderForField(field);
        var descriptor = value.getDescriptorForType();
        var nullability = descriptor.findFieldByName("nullability");
        value.setField(nullability, nullability.getEnumType().findValueByNumber(nullable.getNumber()));
        parameters.forEach((name, parameter) -> value.setField(descriptor.findFieldByName(name), parameter));
        return builder.setField(field, value.build()).build();
    }

    private static int parameter(TypeSignature signature, int index) {
        if (signature.parameters().size() <= index || signature.parameters().get(index).longValue() == null) {
            throw malformed(signature);
        }
        return Math.toIntExact(signature.parameters().get(index).longValue());
    }

    private static void requireChildren(DeclaredType type, int count) {
        if (type.children().size() != count) throw malformed(type.signature());
    }

    private static String canonicalSql(DeclaredType declared) {
        var signature = declared.signature();
        if ("VECTOR".equals(signature.base())) return storageVector(declared).getCanonicalSql();
        if (signature.parameters().isEmpty()) return "REAL".equals(signature.base()) ? "FLOAT" : signature.base();
        var parameters = new ArrayList<String>();
        for (int index = 0; index < signature.parameters().size(); index++) {
            var parameter = signature.parameters().get(index);
            parameters.add(parameter.longValue() != null ? parameter.longValue().toString()
                    : (parameter.fieldName() == null ? "" : "\"" + parameter.fieldName().replace("\"", "\"\"") + "\" ")
                            + canonicalSql(declared.children().get(index)));
        }
        return signature.base() + "(" + String.join(",", parameters) + ")";
    }

    private static IllegalArgumentException malformed(TypeSignature signature) {
        return new IllegalArgumentException("Unsupported or incomplete physical type declaration: " + signature);
    }
}
