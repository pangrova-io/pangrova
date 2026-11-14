package org.pangrova.parser.parse;


import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.ExpressionItem;
import org.pangrova.parser.api.enums.ExpressionType;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;
import org.pangrova.parser.api.ast.expression.ArrayConstructor;
import org.pangrova.parser.api.ast.expression.BetweenExpression;
import org.pangrova.parser.api.ast.expression.BinaryExpression;
import org.pangrova.parser.api.ast.expression.CaseExpression;
import org.pangrova.parser.api.ast.expression.CastExpression;
import org.pangrova.parser.api.ast.expression.ExistsExpression;
import org.pangrova.parser.api.ast.expression.ExtractExpression;
import org.pangrova.parser.api.ast.expression.FunctionCall;
import org.pangrova.parser.api.ast.expression.InExpression;
import org.pangrova.parser.api.ast.expression.InSubqueryExpression;
import org.pangrova.parser.api.ast.expression.IsNullExpression;
import org.pangrova.parser.api.ast.expression.LikeExpression;
import org.pangrova.parser.api.ast.expression.MapConstructor;
import org.pangrova.parser.api.ast.expression.NotExpression;
import org.pangrova.parser.api.ast.expression.NotInExpression;
import org.pangrova.parser.api.ast.expression.NotInSubqueryExpression;
import org.pangrova.parser.api.ast.expression.StarFunctionCall;
import org.pangrova.parser.api.ast.expression.StructConstructor;
import org.pangrova.parser.api.ast.expression.SubqueryExpression;
import org.pangrova.parser.api.ast.expression.SubstringExpression;
import org.pangrova.parser.api.ast.expression.UnaryMinusExpression;
import org.pangrova.parser.api.ast.expression.WindowedFunctionCall;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.WindowSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies that the ExpressionType and StatementType enums are exhaustive:
 * every concrete AST node returns a valid type, and every enum value is reachable.
 */
class AstTypeCompletenessTest {

    private static final Identifier DUMMY_ID = Identifier.of("x");
    private static final Literal DUMMY_LIT = Literal.ofInteger(1);
    private static final Query DUMMY_QUERY = new Query(
            false, List.of(new ExpressionItem(DUMMY_ID)), List.of(), List.of(),
            java.util.Optional.empty(), List.of(), java.util.Optional.empty(),
            java.util.Optional.empty(), List.of(), java.util.Optional.empty(),
            java.util.Optional.empty(), java.util.Optional.empty());

    @Test
    void allExpressionTypesAreCoveredByTypeDispatch() {
        List<Expression> expressions = List.of(
                DUMMY_ID,
                DUMMY_LIT,
                new ArrayConstructor(List.of()),
                new MapConstructor(List.of()),
                new StructConstructor(List.of(StructConstructor.Field.positional(DUMMY_LIT))),
                new BetweenExpression(DUMMY_LIT, DUMMY_LIT, DUMMY_LIT),
                new BinaryExpression(DUMMY_LIT, BinaryExpression.Operator.ADD, DUMMY_LIT),
                new CaseExpression(null, List.of(), java.util.Optional.empty()),
                new CastExpression(DUMMY_LIT, "INT"),
                new ExistsExpression(DUMMY_QUERY),
                new ExtractExpression("YEAR", DUMMY_LIT),
                new FunctionCall(DUMMY_ID, List.of(), false),
                new InExpression(DUMMY_LIT, List.of()),
                new InSubqueryExpression(DUMMY_LIT, DUMMY_QUERY),
                new IsNullExpression(DUMMY_LIT, false),
                new LikeExpression(DUMMY_LIT, DUMMY_LIT),
                new org.pangrova.parser.api.ast.expression.TruthTestExpression(DUMMY_LIT,
                        org.pangrova.parser.api.ast.expression.TruthTestExpression.TruthValue.TRUE, false),
                new NotExpression(DUMMY_LIT),
                new NotInExpression(DUMMY_LIT, List.of()),
                new NotInSubqueryExpression(DUMMY_LIT, DUMMY_QUERY),
                new StarFunctionCall(DUMMY_ID, java.util.Optional.empty()),
                new SubqueryExpression(DUMMY_QUERY),
                new SubstringExpression(DUMMY_LIT, DUMMY_LIT, DUMMY_LIT),
                new UnaryMinusExpression(DUMMY_LIT),
                new WindowedFunctionCall(DUMMY_ID, List.of(), false, new WindowSpec(List.of(), List.of(), null)),
                new org.pangrova.parser.api.ast.expression.ParameterMarker(),
                new org.pangrova.parser.api.ast.expression.SubscriptExpression(DUMMY_ID, DUMMY_LIT)
        );

        Set<ExpressionType> covered = EnumSet.noneOf(ExpressionType.class);
        for (Expression expr : expressions) {
            ExpressionType type = expr.type();
            assertNotNull(type, "type() returned null for " + expr.getClass().getSimpleName());
            covered.add(type);
        }

        Set<ExpressionType> allTypes = EnumSet.allOf(ExpressionType.class);
        Set<ExpressionType> missing = EnumSet.copyOf(allTypes);
        missing.removeAll(covered);

        assertEquals(Set.of(), missing,
                "ExpressionType values not covered by any concrete expression: " + missing);
    }

    @Test
    void exhaustiveSwitchOnExpressionTypeCompiles() {
        // This test verifies that a switch expression over ExpressionType
        // requires all cases. If a new ExpressionType is added without updating
        // this switch, the code will not compile.
        Expression expr = DUMMY_LIT;
        String result = switch (expr.type()) {
            case IDENTIFIER -> "id";
            case LITERAL -> "lit";
            case ARRAY_CONSTRUCTOR -> "array";
            case MAP_CONSTRUCTOR -> "map";
            case STRUCT_CONSTRUCTOR -> "struct";
            case BETWEEN -> "between";
            case BINARY -> "binary";
            case CASE -> "case";
            case CAST -> "cast";
            case EXISTS -> "exists";
            case EXTRACT -> "extract";
            case FUNCTION_CALL -> "func";
            case IN -> "in";
            case IN_SUBQUERY -> "in_sub";
            case IS_NULL -> "null";
            case TRUTH_TEST -> "truth";
            case LIKE -> "like";
            case NOT -> "not";
            case NOT_IN -> "not_in";
            case NOT_IN_SUBQUERY -> "not_in_sub";
            case STAR_FUNCTION_CALL -> "star";
            case SUBQUERY -> "sub";
            case SUBSTRING -> "substr";
            case UNARY_MINUS -> "neg";
            case WINDOWED_FUNCTION_CALL -> "win";
            case PARAMETER_MARKER -> "param";
            case SUBSCRIPT -> "subscript";
        };
        assertEquals("lit", result);
    }

    @Test
    void exhaustiveSwitchOnStatementTypeCompiles() {
        // This test verifies that a switch expression over StatementType
        // requires all cases. If a new StatementType is added without updating
        // this switch, the code will not compile.
        Statement stmt = DUMMY_QUERY;
        String result = switchOnStatementType(stmt.type());
        assertEquals("query", result);
    }

    @Test
    void parsedStatementsReturnCorrectType() {
        AntlrSqlParser parser = new AntlrSqlParser();

        assertEquals(StatementType.QUERY, parser.parse("SELECT 1").type());
        assertEquals(StatementType.CREATE_TABLE, parser.parse("CREATE TABLE t (id INT)").type());
        assertEquals(StatementType.DROP_TABLE, parser.parse("DROP TABLE t").type());
        assertEquals(StatementType.DELETE, parser.parse("DELETE FROM t WHERE id = 1").type());
        assertEquals(StatementType.UPDATE, parser.parse("UPDATE t SET x = 1 WHERE id = 1").type());
        assertEquals(StatementType.EXPLAIN, parser.parse("EXPLAIN SELECT 1").type());
        assertEquals(StatementType.SHOW_CATALOGS, parser.parse("SHOW CATALOGS").type());
    }

    private static String switchOnStatementType(StatementType type) {
        return switch (type) {
            case QUERY -> "query";
            case SET_QUERY -> "set";
            case CTE_QUERY -> "cte";
            case INSERT -> "ins";
            case CREATE_CATALOG -> "cc";
            case CREATE_CONTINUOUS_QUERY -> "ccq";
            case CREATE_DYNAMIC_TABLE -> "cdt";
            case CREATE_MATERIALIZED_VIEW -> "cmv";
            case REFRESH_MATERIALIZED_VIEW -> "rmv";
            case SHOW_SCHEMAS -> "ss";
            case ALTER_CATALOG -> "ac";
            case ALTER_DYNAMIC_TABLE -> "adt";
            case ALTER_MATERIALIZED_VIEW -> "amv";
            case ALTER_EMBEDDING_MODEL -> "aem";
            case ALTER_INDEX -> "ai";
            case ALTER_STAGE -> "astg";
            case ALTER_VOLUME -> "avol";
            case ALTER_TABLE -> "at";
            case BACKFILL_COLUMN -> "bc";
            case CANCEL_BACKFILL -> "cb";
            case CREATE_BRANCH -> "cbr";
            case CREATE_EMBEDDING_MODEL -> "cem";
            case CREATE_INDEX -> "ci";
            case CREATE_SCHEMA -> "cs";
            case CREATE_SEMANTIC_VIEW -> "csv";
            case CREATE_FEATURE_VIEW -> "cfv";
            case CREATE_STAGE -> "cstg";
            case CREATE_VOLUME -> "cvol";
            case CREATE_TABLE -> "ct";
            case CREATE_TABLE_AS_SELECT -> "ctas";
            case CREATE_TAG -> "ctag";
            case CREATE_VIEW -> "cv";
            case DESCRIBE_CATALOG -> "dc";
            case DESCRIBE_DYNAMIC_TABLE -> "ddt";
            case DESCRIBE_MATERIALIZED_VIEW -> "dmv";
            case DESCRIBE_STAGE -> "dstg";
            case DESCRIBE_VOLUME -> "dvol";
            case DESCRIBE_TABLE -> "dt";
            case DROP_BRANCH -> "db";
            case DROP_CATALOG -> "dcat";
            case DROP_DYNAMIC_TABLE -> "ddtab";
            case DROP_EMBEDDING_MODEL -> "dem";
            case DROP_MATERIALIZED_VIEW -> "dmvtab";
            case DROP_INDEX -> "di";
            case DROP_STAGE -> "dstg";
            case DROP_VOLUME -> "dvol";
            case DROP_TABLE -> "dtab";
            case DROP_TAG -> "dtag";
            case DROP_VIEW -> "dv";
            case EXPLAIN -> "exp";
            case GRANT_TENANT -> "gt";
            case INCREMENTAL_READ -> "ir";
            case LIST_STAGE -> "lstg";
            case REMOVE_STAGE -> "rmstg";
            case RECLUSTER_TABLE -> "rt";
            case PUBLISH_SEMANTIC_VIEW -> "psv";
            case PUBLISH_FEATURE_VIEW -> "pfv";
            case MATERIALIZE_FEATURE_VIEW -> "mfv";
            case REVOKE_TENANT -> "rvt";
            case SET_CLUSTERING_ORDER -> "sco";
            case SHOW_BACKFILL_STATUS -> "sbs";
            case SHOW_BRANCHES -> "sbr";
            case SHOW_CATALOGS -> "scat";
            case SHOW_CLUSTERING_STATUS -> "scs";
            case SHOW_DYNAMIC_TABLES -> "sdt";
            case SHOW_EMBEDDING_MODELS -> "sem";
            case SHOW_MATERIALIZATIONS -> "smat";
            case SHOW_MATERIALIZATION_DEPENDENCIES -> "smatdep";
            case SHOW_MATERIALIZATION_GC_HISTORY -> "smatgc";
            case SHOW_MATERIALIZATION_REFRESH_HISTORY -> "smatrf";
            case SHOW_MATERIALIZED_VIEWS -> "smv";
            case SHOW_INDEXES -> "si";
            case SHOW_SEMANTIC_VIEWS -> "ssv";
            case SHOW_STAGES -> "sstg";
            case SHOW_VOLUMES -> "svol";
            case SHOW_METADATA -> "sm";
            case SHOW_TABLES -> "st";
            case SHOW_TAGS -> "stag";
            case SHOW_VIEWS -> "sv";
            case USE_BRANCH -> "ub";
            case DELETE -> "del";
            case INSERT_OVERWRITE -> "io";
            case INSERT_SELECT -> "is";
            case MERGE -> "mrg";
            case UPDATE -> "upd";
            case CALL -> "call";
            case COMPACT -> "cmp";
            case OPTIMIZE -> "opt";
            case VACUUM -> "vac";
            case ANALYZE -> "ana";
            case ALTER_STREAM -> "as";
            case CREATE_STREAM -> "cstr";
            case DESCRIBE_STREAM -> "ds";
            case DROP_STREAM -> "dstr";
            case SHOW_STREAMS -> "sstr";
            case CREATE_STORAGE_PROFILE -> "create_profile";
            case DROP_STORAGE_PROFILE -> "drop_profile";
            case SHOW_STORAGE_PROFILES -> "show_profiles";
            case SHOW_TABLE_CONTROLS -> "show_controls";
            case SHOW_PRIVILEGE_MUTATION -> "privilege_receipt";
            case GRANT_PRIVILEGE -> "grant";
            case REVOKE_PRIVILEGE -> "revoke";
            case SET_SESSION -> "setss";
        };
    }
}
