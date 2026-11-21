package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.StatementType;

import java.time.Duration;

/**
 * Represents {@code CREATE CONTINUOUS QUERY <name> REFRESH EVERY <interval> AS <query>}.
 *
 * <p>A continuous query is created as a durable dynamic table whose target lag is
 * the declared refresh interval. Refresh is interval-driven by the materialization
 * scheduler; it is not watermark-gated.</p>
 */
public record CreateContinuousQueryStatement(
        Identifier name,
        Query query,
        Duration refreshInterval
) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CREATE_CONTINUOUS_QUERY;
    }
}
