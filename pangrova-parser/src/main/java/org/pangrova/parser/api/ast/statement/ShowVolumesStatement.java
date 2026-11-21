package org.pangrova.parser.api.ast.statement;

import java.util.List;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code SHOW VOLUMES [FROM | IN catalog.schema]}.
 *
 * <p>An empty {@code qualifiedName} lists every visible volume.</p>
 */
public record ShowVolumesStatement(List<String> qualifiedName) implements Statement {

    public ShowVolumesStatement {
        qualifiedName = List.copyOf(qualifiedName);
    }

    @Override
    public StatementType type() {
        return StatementType.SHOW_VOLUMES;
    }
}
