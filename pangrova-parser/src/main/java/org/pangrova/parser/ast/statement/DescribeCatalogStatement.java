package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;

/**
 * Represents DESCRIBE CATALOG name statement.
 */
public record DescribeCatalogStatement(String name) implements Statement {
}
