package org.pangrova.planner.validation;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.pangrova.planner.type.ModelBoundVectorRelDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Composite SQL operator table that chains the standard Calcite operators
 * with Pangrova-specific spatial and array functions.
 *
 * <p>Spatial functions ({@code ST_INTERSECTS}, {@code ST_WITHIN},
 * {@code ST_CONTAINS}) accept a column reference plus four numeric
 * bounding-box coordinates and return {@code BOOLEAN NULLABLE}.</p>
 *
 * <p>Array functions ({@code ARRAY_CONTAINS}, {@code ARRAY_CONTAINS_ALL})
 * test element containment and return {@code BOOLEAN NULLABLE}.</p>
 */
public final class PangrovaSqlOperatorTable implements SqlOperatorTable {

    private final SqlOperatorTable standard;
    private final List<SqlOperator> customOperators;

    public PangrovaSqlOperatorTable() {
        this.standard = SqlStdOperatorTable.instance();
        this.customOperators = buildCustomOperators();
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        // Check custom operators first — if we have a match, use it exclusively
        // to override the standard table's return-type/signature (e.g. CHAR_LENGTH
        // must return INTEGER for Velox, not BIGINT from SqlStdOperatorTable).
        List<SqlOperator> customMatches = new java.util.ArrayList<>();
        if (opName.isSimple()) {
            for (SqlOperator op : customOperators) {
                if (nameMatcher.matches(op.getName(), opName.getSimple())) {
                    customMatches.add(op);
                }
            }
        }
        if (!customMatches.isEmpty()) {
            operatorList.addAll(customMatches);
        } else {
            standard.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
        }
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        List<SqlOperator> all = new ArrayList<>(standard.getOperatorList());
        all.addAll(customOperators);
        return List.copyOf(all);
    }

    private static List<SqlOperator> buildCustomOperators() {
        List<SqlOperator> ops = new ArrayList<>();
        addSpatialFunctions(ops);
        ops.add(new GenerateSeriesSqlFunction());
        addArrayAndVectorFunctions(ops);
        addStringFunctions(ops);
        ops.add(new RoundSqlFunction());
        ops.add(typedFunction("TO_IEEE754_32", ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARBINARY, 4),
                        org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE),
                OperandTypes.typeName(SqlTypeName.REAL)));
        ops.add(typedFunction("TO_IEEE754_64", ReturnTypes.cascade(ReturnTypes.explicit(SqlTypeName.VARBINARY, 8),
                        org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE),
                OperandTypes.typeName(SqlTypeName.DOUBLE)));
        ops.add(org.apache.calcite.sql.fun.SqlLibraryOperators.MAX_BY);
        ops.add(org.apache.calcite.sql.fun.SqlLibraryOperators.MIN_BY);
        addCatalogAndMaintenanceFunctions(ops);
        addDdlDmlFunctions(ops);
        addAiAndFileFunctions(ops);
        return List.copyOf(ops);
    }

    /** Registers spatial predicate and constructor functions. */
    private static void addSpatialFunctions(List<SqlOperator> ops) {
        // Spatial predicate functions
        ops.add(spatialPredicateFunction("ST_INTERSECTS"));
        ops.add(spatialPredicateFunction("ST_WITHIN"));
        ops.add(spatialPredicateFunction("ST_CONTAINS"));
        // Spatial constructor functions
        ops.add(spatialConstructorFunction("ST_GEOMFROMTEXT"));
        ops.add(spatialConstructorFunction("ST_GEOGRAPHYFROMTEXT"));
    }

    /** Registers array containment and vector distance functions. */
    private static void addArrayAndVectorFunctions(List<SqlOperator> ops) {
        // Array functions
        ops.add(arrayContains());
        ops.add(arrayContainsAll());
        // Vector distance functions
        ops.add(vectorDistanceFunction("L2_DISTANCE"));
        ops.add(vectorDistanceFunction("COSINE_DISTANCE"));
        ops.add(vectorDistanceFunction("INNER_PRODUCT"));
        ops.add(vectorDistanceFunction("COSINE_SIMILARITY"));
    }

    /** Registers concat and string length functions. */
    private static void addStringFunctions(List<SqlOperator> ops) {
        // Variadic CONCAT function — SqlStdOperatorTable only has binary ||
        // (which rejects 3+ args). This allows CONCAT(a, b, c, ...) to validate.
        ops.add(concatFunction());
        ops.add(new SubstringSqlFunction());
        // String length functions — register all aliases with INTEGER return type
        // to override Calcite's standard CHAR_LENGTH which returns BIGINT.
        // Velox's native 'length' function returns INTEGER.
        ops.add(stringLengthFunction("LENGTH"));
        ops.add(stringLengthFunction("CHARACTER_LENGTH"));
        ops.add(stringLengthFunction("CHAR_LENGTH"));
    }

    /**
     * Registers catalog and maintenance extension functions used by
     * AstToSqlNodeConverter when StatementDispatchPhase falls through.
     */
    private static void addCatalogAndMaintenanceFunctions(List<SqlOperator> ops) {
        ops.add(variadicFunction("COMPACT"));
        ops.add(variadicFunction("OPTIMIZE"));
        ops.add(variadicFunction("VACUUM"));
        ops.add(variadicFunction("CREATE_CATALOG"));
        ops.add(variadicFunction("DROP_CATALOG"));
        ops.add(variadicFunction("SHOW_CATALOGS"));
        ops.add(variadicFunction("DESCRIBE_CATALOG"));
        ops.add(variadicFunction("ALTER_CATALOG"));
        ops.add(variadicFunction("CALL"));
    }

    /** Registers DDL/DML extension functions so Calcite validation passes. */
    private static void addDdlDmlFunctions(List<SqlOperator> ops) {
        // Variant functions
        ops.add(variantGetFunction());
        // Aggregate functions from SqlLibraryOperators (not in SqlStdOperatorTable)
        ops.add(SqlLibraryOperators.ARRAY_AGG);
        ops.add(variadicFunction("CREATE_TABLE"));
        ops.add(variadicFunction("DROP_TABLE"));
        ops.add(variadicFunction("INSERT_INTO"));
        ops.add(variadicFunction("INSERT_OVERWRITE"));
        ops.add(variadicFunction("DELETE_FROM"));
        ops.add(variadicFunction("UPDATE_TABLE"));
        ops.add(variadicFunction("MERGE_INTO"));
        ops.add(variadicFunction("ALTER_TABLE"));
        ops.add(variadicFunction("CREATE_SCHEMA"));
        ops.add(variadicFunction("SHOW_SCHEMAS"));
        ops.add(variadicFunction("SHOW_TABLES"));
        ops.add(variadicFunction("DESCRIBE_TABLE"));
    }

    /** Registers AI inference, AI multimodal and FILE helper functions. */
    private static void addAiAndFileFunctions(List<SqlOperator> ops) {
        var text2 = characterSequence(2);
        var text3 = characterSequence(3);
        var file = OperandTypes.typeName(SqlTypeName.ROW);
        var textFile = OperandTypes.sequence(
                "(<CHARACTER>, <ROW>)", OperandTypes.CHARACTER, file);
        ops.add(typedFunction(
                "AI_COMPLETE",
                ReturnTypes.VARCHAR_2000_NULLABLE,
                OperandTypes.or(
                        text2,
                        text3,
                        OperandTypes.sequence(
                                "(<CHARACTER>, <CHARACTER>, <ROW>)",
                                OperandTypes.CHARACTER, OperandTypes.CHARACTER, file))));
        ops.add(typedFunction("AI_CLASSIFY", ReturnTypes.VARCHAR_2000_NULLABLE, text3));
        ops.add(typedFunction("AI_SUMMARIZE", ReturnTypes.VARCHAR_2000_NULLABLE, text2));
        ops.add(typedFunction("AI_FILTER", ReturnTypes.BOOLEAN_NULLABLE, text3));
        ops.add(new NormalizedAiEmbeddingSqlFunction(vectorReturnType()));
        ops.add(typedFunction("AI_EXTRACT", ReturnTypes.VARCHAR_2000_NULLABLE, text3));
        ops.add(typedFunction("AI_GUARD", ReturnTypes.VARCHAR_2000_NULLABLE, text3));
        ops.add(typedFunction("AI_PREDICT", ReturnTypes.VARCHAR_2000_NULLABLE, text2));
        ops.add(typedFunction("AI_TRANSLATE", ReturnTypes.VARCHAR_2000_NULLABLE, text3));
        ops.add(typedFunction("AI_DESCRIBE_IMAGE", ReturnTypes.VARCHAR_2000_NULLABLE, textFile));
        ops.add(typedFunction("AI_PARSE_DOCUMENT", ReturnTypes.VARCHAR_2000_NULLABLE, textFile));
        ops.add(typedFunction("AI_TRANSCRIBE", ReturnTypes.VARCHAR_2000_NULLABLE, textFile));
        ops.add(typedFunction("AI_OCR", ReturnTypes.VARCHAR_2000_NULLABLE, textFile));
        ops.add(typedFunction("MATCH", ReturnTypes.BOOLEAN_NULLABLE, text2));
        ops.add(typedFunction("BM25_SCORE", ReturnTypes.DOUBLE_NULLABLE, text2));
        ops.add(typedFunction("FILE_FROM_URL", fileReturnType(), OperandTypes.CHARACTER));
        ops.add(typedFunction(
                "FILE_FROM_BYTES",
                fileReturnType(),
                OperandTypes.sequence(
                        "(<BINARY>, <CHARACTER>)", OperandTypes.BINARY, OperandTypes.CHARACTER)));
    }

    private static SqlReturnTypeInference vectorReturnType() {
        return binding -> {
            var factory = binding.getTypeFactory();
            var real = factory.createSqlType(SqlTypeName.REAL);
            return new ModelBoundVectorRelDataType(real, true);
        };
    }

    private static SqlReturnTypeInference fileReturnType() {
        return binding -> {
            var factory = binding.getTypeFactory();
            var varchar = factory.createSqlType(SqlTypeName.VARCHAR);
            var bigint = factory.createSqlType(SqlTypeName.BIGINT);
            var timestamp = factory.createSqlType(SqlTypeName.TIMESTAMP);
            var row = factory.createStructType(
                    List.of(varchar, varchar, bigint, varchar, timestamp),
                    List.of("uri", "mime_type", "size", "checksum", "created_at"));
            return factory.createTypeWithNullability(row, true);
        };
    }

    private static SqlOperandTypeChecker characterSequence(int count) {
        var checkers = new org.apache.calcite.sql.type.SqlSingleOperandTypeChecker[count];
        java.util.Arrays.fill(checkers, OperandTypes.CHARACTER);
        return OperandTypes.sequence("(<CHARACTER>...)", checkers);
    }

    private static SqlFunction typedFunction(
            String name,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeChecker operandTypeChecker) {
        var callback = org.pangrova.protocol.function.declaration.FunctionBindingDeclarations
                .callbackMetadataForSqlName(name);
        if (callback.isPresent()) {
            return new DeclaredCallbackSqlFunction(name, returnTypeInference,
                    operandTypeChecker, callback.orElseThrow());
        }
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * Spatial predicate: {@code (ANY, ANY) → BOOLEAN NULLABLE}.
     * Accepts two geometry arguments (column + geometry value) and returns boolean.
     */
    private static SqlFunction spatialPredicateFunction(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.BOOLEAN_NULLABLE,
                null,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * Spatial constructor: {@code (CHARACTER) → ANY}.
     * Converts WKT text to geometry binary representation.
     */
    private static SqlFunction spatialConstructorFunction(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(org.apache.calcite.sql.type.SqlTypeName.VARBINARY),
                null,
                OperandTypes.family(SqlTypeFamily.CHARACTER),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * {@code ARRAY_CONTAINS(ANY, ANY) → BOOLEAN NULLABLE}.
     */
    private static SqlFunction arrayContains() {
        return new SqlFunction(
                "ARRAY_CONTAINS",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.BOOLEAN_NULLABLE,
                null,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * Vector distance accepts numeric arrays and writes the REAL/DOUBLE signature's explicit casts.
     */
    private static SqlFunction vectorDistanceFunction(String name) {
        return new VectorDistanceSqlFunction(name);
    }

    /**
     * {@code ARRAY_CONTAINS_ALL(ANY, ANY, ...) → BOOLEAN NULLABLE} (variadic).
     */
    private static SqlFunction arrayContainsAll() {
        return new SqlFunction(
                "ARRAY_CONTAINS_ALL",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.BOOLEAN_NULLABLE,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * {@code (CHARACTER) → BIGINT NULLABLE}.
     * Velox (Presto function set) registers {@code length(varchar)} with BIGINT return type.
     * This override ensures Calcite uses BIGINT (not INTEGER) so the Substrait plan
     * matches the native engine's function signature.
     */
    private static SqlFunction stringLengthFunction(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.BIGINT_NULLABLE,
                null,
                OperandTypes.family(SqlTypeFamily.CHARACTER),
                SqlFunctionCategory.STRING);
    }

    /**
     * Permissive {@code ||} concat operator that accepts any CHARACTER family operands.
     * Calcite's standard {@code SqlStdOperatorTable.CONCAT} uses STRING_SAME_SAME which
     * rejects VARCHAR || CHAR(1) combinations that are common in user queries.
     */
    private static SqlFunction concatFunction() {
        return new SqlFunction(
                "CONCAT",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING,
                null,
                OperandTypes.SAME_VARIADIC,
                SqlFunctionCategory.STRING);
    }

    /**
     * {@code VARIANT_GET(ANY, CHARACTER) → VARCHAR NULLABLE}.
     * Extracts a scalar value from a VARIANT (JSON text) column by path expression.
     */
    private static SqlFunction variantGetFunction() {
        return new SqlFunction(
                "VARIANT_GET",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000_NULLABLE,
                null,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    /**
     * Variadic extension function: accepts zero or more arguments, returns VARCHAR.
     * Used for DDL/DML extension operators that bypass normal Calcite validation
     * but must still pass the operator lookup phase.
     */
    private static SqlFunction variadicFunction(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.VARIADIC,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
}
