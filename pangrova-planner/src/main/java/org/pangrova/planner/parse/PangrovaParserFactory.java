package org.pangrova.planner.parse;

import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.parse.SqlParser;
import org.pangrova.parser.api.ast.Statement;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

import java.io.Reader;

/**
 * Calcite SqlParserImplFactory adapter that bridges ANTLR4 parser to Calcite.
 *
 * This factory does NOT return a real SqlAbstractParserImpl.
 * Instead, PangrovaPlanner bypasses Calcite's built-in parser by:
 * 1. Calling AntlrSqlParser.parse(sql) to produce an AST
 * 2. Using AstToSqlNodeConverter to convert AST → Calcite SqlNode tree
 * 3. Feeding the SqlNode directly into Calcite SqlValidator
 */
@org.pangrova.common.concurrency.ThreadSafe
public final class PangrovaParserFactory implements SqlParserImplFactory {

    private final SqlParser parser;
    private static final AstToSqlNodeConverter BOUND_CONVERTER = new AstToSqlNodeConverter(true);

    private final AstToSqlNodeConverter converter;

    public PangrovaParserFactory() {
        this(new AntlrSqlParser());
    }

    /** Uses an explicitly selected parser without exposing implementation state to the adapter. */
    public PangrovaParserFactory(SqlParser parser) {
        this.parser = java.util.Objects.requireNonNull(parser, "parser");
        this.converter = new AstToSqlNodeConverter();
    }

    /**
     * Not used — PangrovaPlanner bypasses Calcite's built-in parser entirely.
     *
     * <p>Design decision: Pangrova uses its own ANTLR4-based parser ({@link AntlrSqlParser})
     * and converts AST to Calcite SqlNode via {@link AstToSqlNodeConverter}. This
     * factory implements {@link SqlParserImplFactory} only to satisfy Calcite's
     * configuration API; the actual parsing entry point is {@link #parseToSqlNode(String)}.
     *
     * @throws UnsupportedOperationException always — use {@link #parseToSqlNode(String)} instead
     */
    @Override
    public SqlAbstractParserImpl getParser(Reader stream) {
        throw new UnsupportedOperationException(
                "Use parseToSqlNode(sql) instead of Calcite's built-in parser");
    }

    /**
     * Parse SQL text through ANTLR4 and convert to Calcite SqlNode.
     */
    public SqlNode parseToSqlNode(String sql) {
        Statement ast = parser.parse(sql);
        return converter.convert(ast);
    }

    /**
     * Convert a pre-parsed AST Statement directly to Calcite SqlNode,
     * bypassing the SQL text → ANTLR parse step. Used for DML statements
     * (DELETE, UPDATE, MERGE) that are already parsed by the statement
     * dispatch layer and need compilation without a SQL string round-trip.
     */
    public SqlNode convertAstToSqlNode(org.pangrova.parser.api.ast.Statement ast) {
        return converter.convert(ast);
    }

    /** Converts an AST whose snapshot selectors were installed in the planning session. */
    public SqlNode convertBoundAstToSqlNode(Statement ast) {
        return BOUND_CONVERTER.convert(ast);
    }

    /**
     * Parse SQL text to AST only (no Calcite conversion).
     */
    public Statement parseToAst(String sql) {
        return parser.parse(sql);
    }
}
