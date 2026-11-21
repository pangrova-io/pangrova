package org.pangrova.parser.api.parse;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.parser.api.exception.ParseException;

import org.pangrova.parser.api.ast.Statement;

/**
 * Thread-safe statement extension. canParse is cheap and side-effect-free; a claimed
 * input must produce a complete AST or ParseException. Delegation uses the portable
 * parser contract, never its implementation, registry, cache or ANTLR objects.
 * Core registry discovery is explicit; construction does not discover providers.
 * Implementations must delegate a different substatement to avoid recursive claims.
 */
@Experimental
public interface StatementParser {

    /**
     * Returns a unique name identifying this statement parser extension.
     *
     * <p>Used for diagnostics and duplicate-registration detection.
     *
     * @return a non-null, non-empty name
     */
    String name();

    /**
     * Returns the priority of this parser. Lower values indicate higher priority.
     * When multiple parsers claim to handle the same SQL, the one with the lowest
     * priority value wins. Default priority is {@code 1000}.
     *
     * @return the priority value
     */
    default int priority() {
        return 1000;
    }

    /**
     * Tests whether this parser can handle the given SQL text.
     *
     * <p>This method must be cheap (typically a prefix/keyword check) and
     * must not have side effects. It is called on every parse invocation
     * to determine which extension parser, if any, should handle the input.
     *
     * @param sql the SQL text to test, never null, already validated for length
     * @return {@code true} if this parser can handle the SQL
     */
    boolean canParse(String sql);

    /**
     * Parses the given SQL text into a {@link Statement} AST node.
     *
     * <p>Called only after {@link #canParse(String)} has returned {@code true}.
     * The {@code parentParser} is provided so extensions can delegate back to
     * the core parser for sub-expressions (e.g., parsing a SELECT query embedded
     * in a custom statement).
     *
     * @param sql          the SQL text to parse
     * @param parentParser the core parser, available for delegating sub-expression parsing
     * @return a non-null {@link Statement}
     * @throws ParseException if the SQL text has syntax errors
     */
    Statement parse(String sql, SqlParser parentParser);
}
