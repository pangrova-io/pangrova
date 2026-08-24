package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents DROP VIEW [IF EXISTS] name.
 */
public record DropViewStatement(
        Identifier viewName,
        boolean ifExists) implements Statement {}
