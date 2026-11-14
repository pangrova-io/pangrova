package org.pangrova.parser.api.parse;

import java.util.List;
import java.util.Objects;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.diagnostic.ParseError;

/** Immutable recovery observation. A partial AST with errors must not be executed. */
public record ParseResult(Statement statement, List<ParseError> errors) {
    public ParseResult {
        errors = List.copyOf(Objects.requireNonNull(errors, "errors"));
        if (errors.isEmpty()) Objects.requireNonNull(statement, "successful statement");
    }
    public boolean isSuccess() { return errors.isEmpty(); }
    public boolean hasErrors() { return !errors.isEmpty(); }
}
