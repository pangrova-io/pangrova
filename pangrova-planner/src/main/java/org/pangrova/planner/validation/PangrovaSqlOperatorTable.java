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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlNameMatcher;

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
        for (SqlOperator op : customOperators) {
            if (nameMatcher.matches(op.getName(), opName.getSimple())) {
                customMatches.add(op);
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
        addArrayAndVectorFunctions(ops);
        addStringFunctions(ops);
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
        // AI inference functions — variadic to support optional model argument
        ops.add(variadicFunction("AI_COMPLETE"));
        ops.add(variadicFunction("AI_CLASSIFY"));
        ops.add(variadicFunction("AI_SUMMARIZE"));
        ops.add(variadicFunction("AI_FILTER"));
        ops.add(variadicFunction("AI_EMBED"));
        ops.add(variadicFunction("AI_EXTRACT"));
        ops.add(variadicFunction("AI_GUARD"));
        ops.add(variadicFunction("AI_PREDICT"));
        ops.add(variadicFunction("AI_TRANSLATE"));
        // AI Multimodal functions — accept FILE struct + optional model name,
        // mirroring the implementations registered in AiFunctionFactory.
        ops.add(variadicFunction("AI_DESCRIBE_IMAGE"));
        ops.add(variadicFunction("AI_PARSE_DOCUMENT"));
        ops.add(variadicFunction("AI_TRANSCRIBE"));
        ops.add(variadicFunction("AI_OCR"));
        // FILE helper constructors — return a FILE struct from a URL or raw bytes.
        ops.add(variadicFunction("FILE_FROM_URL"));
        ops.add(variadicFunction("FILE_FROM_BYTES"));
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
     * Vector distance function: {@code (ANY, ANY) → DOUBLE NULLABLE}.
     * Accepts two array arguments (vector column + query vector literal) and returns distance.
     */
    private static SqlFunction vectorDistanceFunction(String name) {
        return new SqlFunction(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.DOUBLE_NULLABLE,
                null,
                OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
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
