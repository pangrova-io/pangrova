package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents DESCRIBE table_name.
 */
public record DescribeTableStatement(Identifier tableName) implements Statement {}
