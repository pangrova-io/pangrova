package org.pangrova.parser.ast.statement;

import java.util.Optional;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Lists durable refresh attempts, optionally restricted to one materialization.
 *
 * @param object materialization to inspect; empty lists the whole tenant
 */
public record ShowMaterializationRefreshHistoryStatement(Optional<Identifier> object)
        implements Statement {

    public ShowMaterializationRefreshHistoryStatement {
        object = object == null ? Optional.empty() : object;
    }
}
