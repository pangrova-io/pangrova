package org.pangrova.protocol.function;

import org.pangrova.protocol.v2.FunctionOpaqueRegisteredType;

/** Bounds retained diagnostic registration ASTs without inventing a SQL type projection. */
final class FunctionRegisteredTypeAst {
    private static final int MAX_AST_NODES = 4096;
    private static final int MAX_TYPE_NAME_LENGTH = 4096;

    private FunctionRegisteredTypeAst() {}

    static void validate(FunctionOpaqueRegisteredType type) {
        validate(type, new int[]{MAX_AST_NODES}, 0);
    }

    private static void validate(FunctionOpaqueRegisteredType type, int[] remaining, int depth) {
        if (depth > 64 || --remaining[0] < 0 || type.getParametersCount() > 256
                || type.getName().isBlank() || type.getName().length() > MAX_TYPE_NAME_LENGTH
                || (type.hasRowFieldName() && type.getRowFieldName().length() > MAX_TYPE_NAME_LENGTH)
                || !type.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Retained registered type AST is malformed or exceeds its bounds");
        }
        type.getParametersList().forEach(parameter -> validate(parameter, remaining, depth + 1));
    }
}
