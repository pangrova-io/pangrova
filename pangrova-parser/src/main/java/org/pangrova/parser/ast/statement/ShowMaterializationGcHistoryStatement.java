package org.pangrova.parser.ast.statement;

import java.util.Optional;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Lists durable GC decisions, optionally restricted to one materialization.
 *
 * @param object materialization whose generations to inspect; empty lists the whole tenant
 */
public record ShowMaterializationGcHistoryStatement(Optional<Identifier> object)
        implements Statement {

    public ShowMaterializationGcHistoryStatement {
        object = object == null ? Optional.empty() : object;
    }
}
