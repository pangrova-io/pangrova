package org.pangrova.parser.api.ast.statement.stream;

import java.util.Map;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code CREATE STREAM [IF NOT EXISTS] name FROM provider (...)
 * FORMAT '...' INTO table [WITH (...)]}.
 *
 * @param name             stream name
 * @param ifNotExists      whether IF NOT EXISTS was specified
 * @param sourceProvider   source factory identifier
 * @param sourceProperties provider-owned reader settings
 * @param sourceFormat     CDC format identifier (debezium-json, debezium-avro, canal-json, maxwell-json)
 * @param targetTable      fully qualified target Lakehouse table reference
 * @param withProperties   optional stream-level configuration (batch_size, flush_interval, etc.)
 */
public record CreateStreamStatement(
        Identifier name,
        boolean ifNotExists,
        String sourceProvider,
        Map<String, String> sourceProperties,
        String sourceFormat,
        Identifier targetTable,
        Map<String, String> withProperties
) implements Statement {

    public CreateStreamStatement {
        sourceProvider = sourceProvider.toLowerCase(java.util.Locale.ROOT);
        if (!sourceProvider.matches("[a-z][a-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid stream source provider identifier: " + sourceProvider);
        }
        sourceProperties = Map.copyOf(sourceProperties);
        withProperties = Map.copyOf(withProperties);
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_STREAM;
    }
}
