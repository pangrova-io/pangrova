package org.pangrova.parser.ast.statement.stream;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DESCRIBE STREAM <name>}.
 */
public record DescribeStreamStatement(Identifier name) implements Statement {
}
