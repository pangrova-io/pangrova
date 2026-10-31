package org.pangrova.parser.ast.statement;

import java.util.List;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents SHOW VIEWS FROM/IN catalog.schema.
 */
public record ShowViewsStatement(List<String> qualifiedName) implements Statement {

    public ShowViewsStatement {
        qualifiedName = List.copyOf(qualifiedName);
    }

    @Override
    public StatementType type() {
        return StatementType.SHOW_VIEWS;
    }
}
