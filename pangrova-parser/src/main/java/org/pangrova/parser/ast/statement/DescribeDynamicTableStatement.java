package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DESCRIBE DYNAMIC TABLE <name>}.
 */
public record DescribeDynamicTableStatement(Identifier name) implements Statement {
}
