package org.pangrova.parser.parse;

import org.pangrova.parser.api.exception.ParseException;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Regression tests for finding G-PARSE-06 / F-PARSER-02: deeply nested
 * input must surface as a clean {@link ParseException} with
 * {@link PangrovaErrorCode#PARSER_INPUT_TOO_LARGE} regardless of which parser
 * entry point (strict {@link AntlrSqlParser#parse} or
 * {@link AntlrSqlParser#parseWithRecovery}) is used. The historical bug was an
 * uncaught {@link StackOverflowError} that bypassed the documented depth
 * guard because depth was only enforced post-parse via measureDepth().
 *
 * <p>Defense layering verified by these tests:
 * <ol>
 *   <li>An ANTLR ParseTreeListener counts rule entries and aborts at
 *       {@code maxTreeDepth} BEFORE the JVM stack is exhausted.</li>
 *   <li>A {@code StackOverflowError} catch translates any recursion that
 *       slips past the listener into the same ParseException.</li>
 *   <li>The recovery-mode entry point now applies the same guard, where
 *       previously it had none.</li>
 * </ol>
 */
class DeepNestingParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    private static String deeplyNested(int depth) {
        return "(".repeat(depth) + "1" + ")".repeat(depth);
    }

    @Test
    void deeplyNestedParensThrowParseExceptionNotStackOverflow() {
        // 5000 nested parens ≈ 5002 tokens — well under the coarse 65536 token
        // pre-check, yet deep enough to overflow the recursive-descent stack.
        String sql = "SELECT " + deeplyNested(5000);
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_INPUT_TOO_LARGE);
    }

    @Test
    void deeplyNestedParensInRecoveryModeDoNotStackOverflow() {
        String sql = "SELECT " + deeplyNested(5000);
        assertThatThrownBy(() -> parser.parseWithRecovery(sql))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_INPUT_TOO_LARGE);
    }

    @Test
    void moderatelyNestedParensWithinLimitStillParse() {
        // 50 levels — comfortably within the default depth-256 budget.
        assertThatCode(() -> parser.parse("SELECT " + deeplyNested(50)))
                .doesNotThrowAnyException();
    }

    @Test
    void extremelyDeepNestingTriggersStackOverflowSafetyNet() {
        // 50_000 parens — chosen large enough that on a small-stack JVM the listener
        // path may not even get a chance to run before the recursive-descent parser
        // exhausts its stack. The StackOverflowError catch must still translate
        // the failure into a ParseException so the coordinator never sees an Error.
        String sql = "SELECT " + deeplyNested(50_000);
        assertThatThrownBy(() -> parser.parse(sql))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_INPUT_TOO_LARGE);
    }

    @Test
    void boundaryDepthJustBelowConfiguredLimitStillParses() {
        // Custom small limit so we can hit the boundary without paying for thousands
        // of parens; depth=50 sits comfortably under the configured limit of 200.
        AntlrSqlParser bounded = new AntlrSqlParser(AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH, 200);
        assertThatCode(() -> bounded.parse("SELECT " + deeplyNested(50)))
                .doesNotThrowAnyException();
    }

    @Test
    void boundaryDepthAtConfiguredLimitFailsCleanly() {
        // depth=200 produces a parse-tree depth above 200 (each paren adds rule
        // entries for paren_expr/expression/primary), so this MUST fail with a
        // clean ParseException, not a StackOverflowError.
        AntlrSqlParser bounded = new AntlrSqlParser(AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH, 200);
        String sql = "SELECT " + deeplyNested(200);
        assertThatThrownBy(() -> bounded.parse(sql))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_INPUT_TOO_LARGE);
    }

    @Test
    void boundaryDepthAtConfiguredLimitFailsCleanlyInRecoveryMode() {
        AntlrSqlParser bounded = new AntlrSqlParser(AntlrSqlParser.DEFAULT_MAX_SQL_LENGTH, 200);
        String sql = "SELECT " + deeplyNested(200);
        assertThatThrownBy(() -> bounded.parseWithRecovery(sql))
                .isInstanceOf(ParseException.class)
                .hasFieldOrPropertyWithValue("errorCode", PangrovaErrorCode.PARSER_INPUT_TOO_LARGE);
    }
}
