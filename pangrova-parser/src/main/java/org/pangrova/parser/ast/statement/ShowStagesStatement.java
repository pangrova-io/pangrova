package org.pangrova.parser.ast.statement;

import java.util.List;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents {@code SHOW STAGES [FROM | IN catalog.schema]}.
 *
 * <p>An empty {@code qualifiedName} lists every visible stage.</p>
 */
public record ShowStagesStatement(List<String> qualifiedName) implements Statement {

    public ShowStagesStatement {
        qualifiedName = List.copyOf(qualifiedName);
    }

    @Override
    public StatementType type() {
        return StatementType.SHOW_STAGES;
    }
}
