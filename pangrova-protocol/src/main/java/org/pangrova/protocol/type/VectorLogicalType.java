package org.pangrova.protocol.type;

import io.substrait.proto.Type;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.TypeConstraint;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Canonical cross-runtime VECTOR contract; the AI embedding entry point remains strict FLOAT32. */
public final class VectorLogicalType {

    public static final int MAX_DIMENSION = 65_535;
    public static final String DIMENSION_CONSTRAINT = "dimension";
    public static final String ELEMENT_TYPE_CONSTRAINT = "element_type";
    public static final String FLOAT32 = "FLOAT32";
    public static final String FLOAT64 = "FLOAT64";

    private static final Pattern CANONICAL = Pattern.compile("VECTOR\\(([1-9][0-9]*)\\)");

    private VectorLogicalType() {}

    /** Creates the only canonical logical contract for a dense float embedding. */
    public static LogicalType create(int dimension) {
        return create(dimension, FLOAT32, true, false);
    }

    /** Preserves storage element precision and both levels of declared nullability. */
    public static LogicalType create(int dimension, String elementType,
                                     boolean nullable, boolean elementNullable) {
        requireDimension(dimension);
        Type.Nullability childNullability = nullability(elementNullable);
        Type element = switch (elementType) {
            case FLOAT32 -> Type.newBuilder().setFp32(Type.FP32.newBuilder()
                    .setNullability(childNullability)).build();
            case FLOAT64 -> Type.newBuilder().setFp64(Type.FP64.newBuilder()
                    .setNullability(childNullability)).build();
            default -> throw new IllegalArgumentException("Unsupported VECTOR element type: " + elementType);
        };
        Type physical = Type.newBuilder()
                .setList(Type.List.newBuilder()
                        .setType(element)
                        .setNullability(nullability(nullable)))
                .build();
        return LogicalType.newBuilder()
                .setCanonicalSql("VECTOR(" + dimension + (FLOAT64.equals(elementType) ? ",DOUBLE" : "") + ")")
                .setSubstraitType(physical)
                .addConstraints(TypeConstraint.newBuilder()
                        .setName(DIMENSION_CONSTRAINT)
                        .setIntegerValue(dimension))
                .addConstraints(TypeConstraint.newBuilder()
                        .setName(ELEMENT_TYPE_CONSTRAINT)
                        .setStringValue(elementType))
                .build();
    }

    /** Validates the complete storage contract without weakening the AI-specific validator. */
    public static int requireStorageDimension(LogicalType type) {
        if (type == null || !type.hasSubstraitType() || !type.getSubstraitType().hasList()) {
            throw new IllegalArgumentException("Storage VECTOR requires a declared list type");
        }
        var list = type.getSubstraitType().getList();
        var child = list.getType();
        String element = child.hasFp32() ? FLOAT32 : child.hasFp64() ? FLOAT64 : "";
        var childNullability = child.hasFp32() ? child.getFp32().getNullability()
                : child.hasFp64() ? child.getFp64().getNullability() : Type.Nullability.NULLABILITY_UNSPECIFIED;
        int dimension = Math.toIntExact(type.getConstraintsList().stream()
                .filter(constraint -> DIMENSION_CONSTRAINT.equals(constraint.getName()))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("VECTOR dimension is missing"))
                .getIntegerValue());
        requireDeclaredNullability(list.getNullability());
        requireDeclaredNullability(childNullability);
        if (!type.equals(create(dimension, element,
                list.getNullability() == Type.Nullability.NULLABILITY_NULLABLE,
                childNullability == Type.Nullability.NULLABILITY_NULLABLE))) {
            throw new IllegalArgumentException("Storage VECTOR declaration differs from its canonical contract");
        }
        return dimension;
    }

    private static Type.Nullability nullability(boolean nullable) {
        return nullable ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED;
    }

    private static void requireDeclaredNullability(Type.Nullability value) {
        if (value != Type.Nullability.NULLABILITY_NULLABLE && value != Type.Nullability.NULLABILITY_REQUIRED) {
            throw new IllegalArgumentException("VECTOR nullability must be explicitly declared");
        }
    }

    /** Validates the full canonical type and returns its fixed dimension. */
    public static int requireDimension(LogicalType type) {
        if (type == null) {
            throw new IllegalArgumentException("VECTOR logical type must not be null");
        }
        Matcher matcher = CANONICAL.matcher(type.getCanonicalSql());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "VECTOR logical type is not canonical: " + type.getCanonicalSql());
        }
        final int dimension;
        try {
            dimension = Integer.parseInt(matcher.group(1));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("VECTOR dimension exceeds integer range", failure);
        }
        requireDimension(dimension);
        if (!type.equals(create(dimension))) {
            throw new IllegalArgumentException(
                    "VECTOR logical type constraints or physical encoding differ from canonical contract");
        }
        return dimension;
    }

    private static void requireDimension(int dimension) {
        if (dimension < 1 || dimension > MAX_DIMENSION) {
            throw new IllegalArgumentException(
                    "VECTOR dimension must be in [1, " + MAX_DIMENSION + "]: " + dimension);
        }
    }
}
