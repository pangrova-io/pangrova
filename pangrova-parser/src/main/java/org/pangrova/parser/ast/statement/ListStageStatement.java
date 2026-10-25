package org.pangrova.parser.ast.statement;

import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code LIST @stage[/path] [PATTERN = 'regex']}.
 *
 * @param name    the stage reference
 * @param path    stage-relative prefix, without leading or trailing slash
 * @param pattern optional regular expression applied to the relative path
 */
public record ListStageStatement(Identifier name, Optional<String> path,
        Optional<String> pattern) implements Statement {

    public ListStageStatement {
        path = path == null ? Optional.empty() : path;
        pattern = pattern == null ? Optional.empty() : pattern;
    }
}
