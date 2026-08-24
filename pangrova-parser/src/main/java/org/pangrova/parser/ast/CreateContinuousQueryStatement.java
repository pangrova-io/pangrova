package org.pangrova.parser.ast;

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
}
