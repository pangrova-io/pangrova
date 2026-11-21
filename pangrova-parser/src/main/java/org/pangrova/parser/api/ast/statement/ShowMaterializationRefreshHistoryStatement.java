package org.pangrova.parser.api.ast.statement;

import java.util.Optional;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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

    @Override
    public StatementType type() {
        return StatementType.SHOW_MATERIALIZATION_REFRESH_HISTORY;
    }
}
