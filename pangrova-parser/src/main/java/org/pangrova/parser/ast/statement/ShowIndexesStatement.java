package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents SHOW INDEXES FROM/ON tableName.
 */
public record ShowIndexesStatement(Identifier tableName) implements Statement {
}
