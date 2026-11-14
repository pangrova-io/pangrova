package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.RefreshStrategy;
import org.pangrova.parser.api.enums.StatementType;

import java.time.Duration;

/**
 * Represents {@code CREATE CONTINUOUS QUERY <name> AS <query> REFRESH EVERY <interval>}.
 *
 * <p>A continuous query is a Dynamic Table with {@link RefreshStrategy#CONTINUOUS}
 * that auto-refreshes based on watermark advance at the specified interval.</p>
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
