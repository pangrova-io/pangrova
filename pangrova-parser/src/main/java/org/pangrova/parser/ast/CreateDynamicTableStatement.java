package org.pangrova.parser.ast;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Represents {@code CREATE DYNAMIC TABLE <name> TARGET_LAG = '<duration>'
 * [REFRESH_MODE = <mode>] [WAREHOUSE = '<warehouse>'] AS <query>
 * [WITH (<properties>)]}.
 */
public record CreateDynamicTableStatement(
        Identifier name,
        Query query,
        Duration targetLag,
        Optional<RefreshMode> refreshMode,
        Optional<String> warehouse,
        Map<String, String> properties
) implements Statement {

    public CreateDynamicTableStatement {
        properties = Map.copyOf(properties);
    }
}
