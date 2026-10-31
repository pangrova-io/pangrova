package org.pangrova.parser.ast.statement.stream;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code SHOW STREAMS}.
 */
public record ShowStreamsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_STREAMS;
    }
}
