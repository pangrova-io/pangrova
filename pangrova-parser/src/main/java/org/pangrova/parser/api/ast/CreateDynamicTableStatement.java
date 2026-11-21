package org.pangrova.parser.api.ast;

import org.pangrova.parser.api.enums.IncrementalStrategy;
import org.pangrova.parser.api.enums.RefreshMode;
import org.pangrova.parser.api.enums.StatementType;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Represents {@code CREATE DYNAMIC TABLE <name> ... AS <query>}.
 *
 * @param name object name
 * @param query defining query
 * @param targetLag freshness target; zero means manual refresh
 * @param refreshMode configured refresh mode
 * @param warehouse optional warehouse policy
 * @param properties planning and execution properties
 * @param incrementalStrategy pinned incremental strategy; empty means automatic
 */
public record CreateDynamicTableStatement(
        Identifier name,
        Query query,
        Duration targetLag,
        Optional<RefreshMode> refreshMode,
        Optional<String> warehouse,
        Map<String, String> properties,
        Optional<IncrementalStrategy> incrementalStrategy
) implements Statement {

    public CreateDynamicTableStatement {
        properties = Map.copyOf(properties);
        incrementalStrategy = incrementalStrategy == null
                ? Optional.empty() : incrementalStrategy;
    }

    public CreateDynamicTableStatement(Identifier name, Query query, Duration targetLag,
            Optional<RefreshMode> refreshMode, Optional<String> warehouse,
            Map<String, String> properties) {
        this(name, query, targetLag, refreshMode, warehouse, properties, Optional.empty());
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_DYNAMIC_TABLE;
    }
}
