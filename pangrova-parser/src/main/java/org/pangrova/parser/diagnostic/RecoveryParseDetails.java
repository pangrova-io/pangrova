package org.pangrova.parser.diagnostic;

import java.util.List;
import java.util.Objects;
import org.antlr.v4.runtime.tree.ParseTree;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.diagnostic.ParseError;
import org.pangrova.parser.api.parse.ParseResult;

/**
 * Explicit ANTLR diagnostic view for Core tooling. The tree is invocation-local and
 * mutable, not a thread-safe shared value; it never enters the portable API result.
 * Mutating the tree does not mutate the separately built AST or diagnostic list.
 */
public record RecoveryParseDetails(ParseResult result, ParseTree partialTree) {
    public RecoveryParseDetails {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(partialTree, "partialTree");
    }
    public static RecoveryParseDetails success(Statement statement, ParseTree tree) {
        return new RecoveryParseDetails(new ParseResult(statement, List.of()), tree);
    }
    public static RecoveryParseDetails withErrors(Statement statement, ParseTree tree, List<ParseError> errors) {
        if (errors.isEmpty()) throw new IllegalArgumentException("errors must not be empty for error result");
        return new RecoveryParseDetails(new ParseResult(statement, errors), tree);
    }
}
