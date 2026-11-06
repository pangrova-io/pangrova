package org.pangrova.parser.api.ast.statement;

import java.util.Optional;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

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

    @Override
    public StatementType type() {
        return StatementType.SHOW_MATERIALIZATION_GC_HISTORY;
    }
}
