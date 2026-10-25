package org.pangrova.parser.ast.statement;

import java.util.List;

import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code SHOW VOLUMES [FROM | IN catalog.schema]}.
 *
 * <p>An empty {@code qualifiedName} lists every visible volume.</p>
 */
public record ShowVolumesStatement(List<String> qualifiedName) implements Statement {

    public ShowVolumesStatement {
        qualifiedName = List.copyOf(qualifiedName);
    }
}
