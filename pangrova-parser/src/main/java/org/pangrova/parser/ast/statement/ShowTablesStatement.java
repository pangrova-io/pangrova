package org.pangrova.parser.ast.statement;

import java.util.List;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

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
