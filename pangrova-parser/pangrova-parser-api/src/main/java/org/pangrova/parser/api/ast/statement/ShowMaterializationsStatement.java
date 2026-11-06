package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;

/** Lists every materialization object of the current tenant. */
public record ShowMaterializationsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_MATERIALIZATIONS;
    }
}
