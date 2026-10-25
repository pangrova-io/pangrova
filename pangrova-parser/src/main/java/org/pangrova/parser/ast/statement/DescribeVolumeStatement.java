package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code DESCRIBE VOLUME name}.
 */
public record DescribeVolumeStatement(Identifier name) implements Statement {
}
