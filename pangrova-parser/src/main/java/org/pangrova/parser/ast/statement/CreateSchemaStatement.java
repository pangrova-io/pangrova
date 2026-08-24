package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents CREATE SCHEMA [IF NOT EXISTS] qualifiedName.
 */
public record CreateSchemaStatement(Identifier schemaName, boolean ifNotExists) implements Statement {}
