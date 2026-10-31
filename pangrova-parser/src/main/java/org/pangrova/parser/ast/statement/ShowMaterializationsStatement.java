package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/** Lists every materialization object of the current tenant. */
public record ShowMaterializationsStatement() implements Statement {

    @Override
    public StatementType type() {
        return StatementType.SHOW_MATERIALIZATIONS;
    }
}
