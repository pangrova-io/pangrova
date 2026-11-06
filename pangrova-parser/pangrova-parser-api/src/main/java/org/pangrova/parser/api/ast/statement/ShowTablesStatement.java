package org.pangrova.parser.api.ast.statement;

import java.util.List;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/**
 * Represents SHOW TABLES FROM/IN catalog.schema.
 */
public record ShowTablesStatement(List<String> qualifiedName) implements Statement {

    public ShowTablesStatement {
        qualifiedName = List.copyOf(qualifiedName);
    }

    @Override
    public StatementType type() {
        return StatementType.SHOW_TABLES;
    }
}
