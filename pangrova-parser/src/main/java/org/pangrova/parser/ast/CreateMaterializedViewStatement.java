package org.pangrova.parser.ast;

import java.time.Duration;
import java.util.Optional;

/**
 * Represents {@code CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] <name> ... AS <query>}.
 *
 * @param name object name
 * @param query defining query
 * @param targetLag freshness target; zero means manual refresh
 * @param refreshMode configured refresh mode
 * @param orReplace whether an existing object is replaced atomically
 * @param ifNotExists whether an existing object is left untouched
 * @param incrementalStrategy pinned incremental strategy; empty means automatic
 */
public record CreateMaterializedViewStatement(
        Identifier name,
        Query query,
        Duration targetLag,
        Optional<RefreshMode> refreshMode,
        boolean orReplace,
        boolean ifNotExists,
        Optional<IncrementalStrategy> incrementalStrategy
) implements Statement {

    public CreateMaterializedViewStatement {
        targetLag = targetLag == null ? Duration.ZERO : targetLag;
        refreshMode = refreshMode == null ? Optional.empty() : refreshMode;
        incrementalStrategy = incrementalStrategy == null
                ? Optional.empty() : incrementalStrategy;
        if (targetLag.isNegative()) {
            throw new IllegalArgumentException("target lag must not be negative");
        }
        if (orReplace && ifNotExists) {
            throw new IllegalArgumentException("OR REPLACE and IF NOT EXISTS are mutually exclusive");
        }
    }

    public CreateMaterializedViewStatement(Identifier name, Query query, Duration targetLag,
            Optional<RefreshMode> refreshMode) {
        this(name, query, targetLag, refreshMode, false, false, Optional.empty());
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_MATERIALIZED_VIEW;
    }
}
