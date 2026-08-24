package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.mv.MaterializedViewMetadata;
import org.pangrova.metadata.mv.MaterializedViewStore;
import org.pangrova.parser.ast.RefreshStrategy;
import org.pangrova.planner.mv.MaterializedViewDefinition;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Recovers materialized view definitions from the persistent metadata store
 * and registers them into the planner's {@link MaterializedViewRegistry}
 * so they are available for query rewriting after Coordinator restart.
 *
 * <p>Only INCREMENTAL strategy entries (Dynamic Tables) are recovered.
 * Non-INCREMENTAL entries (ON_SCHEDULE, ON_COMMIT) are legacy MV entries
 * that should not be recovered.
 */
public final class MaterializedViewRecovery {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewRecovery.class);

    private final MaterializedViewStore store;
    private final MaterializedViewRegistry manager;

    public MaterializedViewRecovery(MaterializedViewStore store, MaterializedViewRegistry manager) {
        this.store = store;
        this.manager = manager;
    }

    /**
     * Loads active MV metadata with INCREMENTAL strategy from the persistent
     * store and registers them into the in-memory MV manager for query rewriting.
     *
     * @return the number of MVs recovered
     */
    public int recover() {
        List<MaterializedViewMetadata> activeMvs = store.listActive();
        if (activeMvs.isEmpty()) {
            LOG.info("action=mv_recovery_complete recovered=0");
            return 0;
        }

        List<MaterializedViewMetadata> incrementalMvs = activeMvs.stream()
                .filter(mv -> "INCREMENTAL".equals(mv.refreshStrategy()))
                .toList();

        if (incrementalMvs.isEmpty()) {
            LOG.info("action=mv_recovery_complete recovered=0 skipped={}", activeMvs.size());
            return 0;
        }

        List<MaterializedViewDefinition> definitions = incrementalMvs.stream()
                .map(MaterializedViewRecovery::toDefinition)
                .toList();

        manager.registerAll(definitions);
        LOG.info("action=mv_recovery_complete recovered={} skipped={}",
                definitions.size(), activeMvs.size() - definitions.size());
        return definitions.size();
    }

    /**
     * Converts persistent metadata to planner-domain definition.
     */
    static MaterializedViewDefinition toDefinition(MaterializedViewMetadata metadata) {
        RefreshStrategy strategy = parseStrategy(metadata.refreshStrategy());
        return new MaterializedViewDefinition(
                metadata.mvName(),
                metadata.baseQuery(),
                strategy,
                true,
                Optional.ofNullable(metadata.lastRefreshedAt()),
                metadata.createdAt());
    }

    private static RefreshStrategy parseStrategy(String strategyName) {
        try {
            return RefreshStrategy.valueOf(strategyName);
        } catch (IllegalArgumentException e) {
            LOG.warn("action=mv_recovery_unknown_strategy strategy={} fallback=INCREMENTAL", strategyName);
            return RefreshStrategy.INCREMENTAL;
        }
    }
}
