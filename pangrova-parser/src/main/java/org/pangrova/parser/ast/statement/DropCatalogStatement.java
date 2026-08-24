package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP CATALOG name statement.
 */
public record DropCatalogStatement(String name) implements Statement {
}
