package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;

/** Lists every materialization object of the current tenant. */
public record ShowMaterializationsStatement() implements Statement {
}
