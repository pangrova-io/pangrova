package org.pangrova.parser.api.ast.statement.stream;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents {@code SHOW STREAMS}.
 */
public record ShowStreamsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_STREAMS;
    }
}
