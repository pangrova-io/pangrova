package org.pangrova.parser.api.ast.statement;

import java.util.List;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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
