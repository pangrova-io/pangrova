package org.pangrova.parser;

import org.pangrova.parser.ast.Statement;

/**
 * Extension point for custom SQL statement parsers.
 *
 * <p>Downstream modules (connectors, plugins) can implement this interface to
 * add support for custom statement types without modifying the core ANTLR grammar.
 * Implementations are discovered via {@link java.util.ServiceLoader} at
 * {@link StatementParserRegistry} construction time, or registered programmatically
 * via {@link StatementParserRegistry#register(StatementParser)}.
 *
 * <p>When {@link SqlParser#parse(String)} is invoked, the registered extension
 * parsers are consulted first (in priority order). If any extension parser's
 * {@link #canParse(String)} returns {@code true}, its {@link #parse(String, SqlParser)}
 * method is called. If no extension parser claims the input, the core ANTLR grammar
 * handles the parse as usual.
 *
 * <p><b>Contract:</b>
 * <ul>
 *   <li>{@link #canParse(String)} must be cheap and side-effect-free; it is called
 *       on every parse attempt.</li>
 *   <li>If {@code canParse} returns {@code true}, {@code parse} must either return
 *       a valid {@link Statement} or throw {@link ParseException}.</li>
 *   <li>Implementations must be thread-safe.</li>
 * </ul>
 *
 * @see StatementParserRegistry
 * @see SqlParser
 */
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
