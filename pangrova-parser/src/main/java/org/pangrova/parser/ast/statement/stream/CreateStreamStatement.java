package org.pangrova.parser.ast.statement.stream;

import java.util.Map;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code CREATE STREAM [IF NOT EXISTS] name FROM KAFKA (...)
 * FORMAT '...' INTO table [WITH (...)]}.
 *
 * @param name             stream name
 * @param ifNotExists      whether IF NOT EXISTS was specified
 * @param sourceProperties Kafka source properties (bootstrap.servers, topic, group.id)
 * @param sourceFormat     CDC format identifier (debezium-json, debezium-avro, canal-json, maxwell-json)
 * @param targetTable      fully qualified target Lakehouse table reference
 * @param withProperties   optional stream-level configuration (batch_size, flush_interval, etc.)
 */
public record CreateStreamStatement(
        Identifier name,
        boolean ifNotExists,
        Map<String, String> sourceProperties,
        String sourceFormat,
        Identifier targetTable,
        Map<String, String> withProperties
) implements Statement {

    public CreateStreamStatement {
        sourceProperties = Map.copyOf(sourceProperties);
        withProperties = Map.copyOf(withProperties);
    }
}
