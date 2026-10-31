package org.pangrova.parser.ast.statement;

import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code REMOVE @stage[/path] [PATTERN = 'regex']}.
 *
 * @param name    the stage reference
 * @param path    stage-relative prefix, without leading or trailing slash
 * @param pattern optional regular expression applied to the relative path
 */
public record RemoveStageStatement(Identifier name, Optional<String> path,
        Optional<String> pattern) implements Statement {

    public RemoveStageStatement {
        path = path == null ? Optional.empty() : path;
        pattern = pattern == null ? Optional.empty() : pattern;
    }

    @Override
    public StatementType type() {
        return StatementType.REMOVE_STAGE;
    }
}
