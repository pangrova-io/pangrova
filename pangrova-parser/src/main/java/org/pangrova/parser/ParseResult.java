package org.pangrova.parser;

import java.util.List;

import org.pangrova.parser.ast.Statement;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * Result of parsing SQL in error-recovery mode. Contains either a successfully
 * built AST statement (possibly partial) alongside any syntax errors encountered.
 *
 * <p>When no errors are present, the statement is a complete, valid AST.
 * When errors are present, the statement may be null (if the parse tree was
 * too damaged to build any AST) or a partial AST representing the portions
 * of the SQL that were successfully parsed.
 *
 * <p>The partial parse tree is always available when parsing completes
 * (ANTLR recovery mode always produces a tree), enabling IDE-style
 * incremental analysis even on invalid SQL.
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class ParseResult {

    private final Statement statement;
    private final ParseTree partialTree;
    private final List<ParseError> errors;

    private ParseResult(Statement statement, ParseTree partialTree, List<ParseError> errors) {
        this.statement = statement;
        this.partialTree = partialTree;
        this.errors = List.copyOf(errors);
    }

    /**
     * Creates a successful parse result with no errors.
     */
    static ParseResult success(Statement statement, ParseTree tree) {
        return new ParseResult(statement, tree, List.of());
    }

    /**
     * Creates a parse result with errors and an optional partial statement.
     *
     * @param statement   the partial AST, or null if no AST could be built
     * @param partialTree the ANTLR parse tree (always non-null in recovery mode)
     * @param errors      non-empty list of syntax errors encountered
     */
    static ParseResult withErrors(Statement statement, ParseTree partialTree, List<ParseError> errors) {
        if (errors.isEmpty()) {
            throw new IllegalArgumentException("errors must not be empty for error result");
        }
        return new ParseResult(statement, partialTree, errors);
    }

    /**
     * Returns true if parsing completed without any syntax errors.
     */
    public boolean isSuccess() {
        return errors.isEmpty();
    }

    /**
     * Returns true if syntax errors were encountered during parsing.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /**
     * Returns the parsed statement, or null if the SQL was too malformed
     * to produce any AST.
     */
    public Statement statement() {
        return statement;
    }

    /**
     * Returns the ANTLR parse tree produced by error-recovery parsing.
     * Always non-null; may contain error nodes for invalid portions.
     */
    public ParseTree partialTree() {
        return partialTree;
    }

    /**
     * Returns an unmodifiable list of syntax errors encountered during parsing.
     * Empty if parsing was successful.
     */
    public List<ParseError> errors() {
        return errors;
    }
}
