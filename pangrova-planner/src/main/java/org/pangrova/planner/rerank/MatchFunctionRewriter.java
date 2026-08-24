package org.pangrova.planner.rerank;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.expression.FunctionCall;

import java.util.List;
import java.util.Optional;

/**
 * Detects MATCH(column, 'query') function calls in AST expressions and extracts
 * the information needed to produce a {@link FullTextScoringNode}.
 *
 * <p>MATCH() returns a DOUBLE BM25 score (not a BOOLEAN). When used in WHERE,
 * it acts as a filter (score &gt; 0 means the document matches). When used in SELECT,
 * it exposes the relevance score for ordering or display.</p>
 *
 * <p>This rewriter is stateless and thread-safe.</p>
 */
public final class MatchFunctionRewriter {

    private static final String MATCH_FUNCTION_NAME = "MATCH";

    private MatchFunctionRewriter() {}

    /**
     * Checks whether the given expression is a MATCH() function call.
     *
     * @param expression AST expression to inspect
     * @return true if the expression is a MATCH(column, 'query') call
     */
    public static boolean isMatchFunction(Expression expression) {
        return extractMatchCall(expression).isPresent();
    }

    /**
     * Extracts MATCH() call details from an expression.
     *
     * @param expression AST expression to inspect
     * @return match details if the expression is a valid MATCH(column, 'query'), empty otherwise
     */
    public static Optional<MatchCallDetails> extractMatchCall(Expression expression) {
        if (!(expression instanceof FunctionCall fn)) {
            return Optional.empty();
        }
        if (!isMatchName(fn.name())) {
            return Optional.empty();
        }
        List<Expression> args = fn.arguments();
        if (args.size() != 2) {
            return Optional.empty();
        }

        String column = extractColumnName(args.get(0));
        String queryText = extractStringLiteral(args.get(1));
        if (column == null || queryText == null) {
            return Optional.empty();
        }

        return Optional.of(new MatchCallDetails(column, queryText));
    }

    /**
     * Creates a {@link FullTextScoringNode} from a MATCH() call in the context of a table.
     *
     * @param tableName fully qualified table name
     * @param details   extracted MATCH() call details
     * @param preFilters serialized pre-filter predicates from the WHERE clause
     * @return the FTS scoring plan node
     */
    public static FullTextScoringNode toScoringNode(String tableName,
                                                    MatchCallDetails details,
                                                    List<String> preFilters) {
        return new FullTextScoringNode(tableName, details.column(), details.queryText(), preFilters);
    }

    private static boolean isMatchName(Identifier name) {
        List<String> parts = name.parts();
        return parts.size() == 1 && MATCH_FUNCTION_NAME.equalsIgnoreCase(parts.get(0));
    }

    private static String extractColumnName(Expression expr) {
        if (expr instanceof Identifier id) {
            List<String> parts = id.parts();
            return parts.isEmpty() ? null : String.join(".", parts);
        }
        return null;
    }

    private static String extractStringLiteral(Expression expr) {
        if (expr instanceof Literal lit && lit.value() instanceof LiteralValue.StringValue sv) {
            return sv.value();
        }
        return null;
    }

    /**
     * Extracted details from a MATCH(column, 'query') function call.
     *
     * @param column    the column name to search
     * @param queryText the query text for BM25 scoring
     */
    public record MatchCallDetails(String column, String queryText) {

        public MatchCallDetails {
            if (column == null || column.isBlank()) {
                throw new IllegalArgumentException("column must not be blank");
            }
            if (queryText == null || queryText.isBlank()) {
                throw new IllegalArgumentException("queryText must not be blank");
            }
        }
    }
}
