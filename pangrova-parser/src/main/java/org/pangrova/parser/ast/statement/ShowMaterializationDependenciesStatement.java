package org.pangrova.parser.ast.statement;

import java.util.Optional;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Lists durable dependency edges, optionally restricted to one object.
 *
 * @param object materialization whose edges to inspect; empty lists the whole tenant
 */
public record ShowMaterializationDependenciesStatement(Optional<Identifier> object)
        implements Statement {

    public ShowMaterializationDependenciesStatement {
        object = object == null ? Optional.empty() : object;
    }
}
