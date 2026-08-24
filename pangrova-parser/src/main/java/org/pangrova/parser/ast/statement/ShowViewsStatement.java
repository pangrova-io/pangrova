package org.pangrova.parser.ast.statement;

import java.util.List;

import org.pangrova.parser.ast.Statement;

/**
 * Represents SHOW VIEWS FROM/IN catalog.schema.
 */
public record ShowViewsStatement(List<String> qualifiedName) implements Statement {

    public ShowViewsStatement {
        qualifiedName = List.copyOf(qualifiedName);
    }
}
