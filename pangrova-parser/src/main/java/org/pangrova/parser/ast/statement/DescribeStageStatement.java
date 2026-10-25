package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DESCRIBE STAGE name}.
 */
public record DescribeStageStatement(Identifier name) implements Statement {
}
