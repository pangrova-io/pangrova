package org.pangrova.coordinator.metadata;

import org.pangrova.metadata.mv.MaterializedViewMetadata;
import org.pangrova.metadata.mv.MaterializedViewStore;
import org.pangrova.metadata.mv.MaterializedViewState;
import org.pangrova.planner.mv.MaterializedViewDefinition;
import org.pangrova.planner.mv.MaterializedViewRegistry.MaterializedViewLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Bridges MV lifecycle events from the planner's {@link org.pangrova.planner.mv.MaterializedViewRegistry}
 * to the metadata store for durable persistence.
 * <p>
 * Converts between planner-domain {@link MaterializedViewDefinition} and
 * metadata-domain {@link MaterializedViewMetadata}.
 */
public final class MaterializedViewPersistenceListener implements MaterializedViewLifecycleListener {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewPersistenceListener.class);

    private final MaterializedViewStore store;

    public MaterializedViewPersistenceListener(MaterializedViewStore store) {
        this.store = store;
    }

    @Override
    public void onCreated(MaterializedViewDefinition definition) {
        MaterializedViewMetadata metadata = toMetadata(definition, MaterializedViewState.CREATED);
        store.save(metadata);
        LOG.info("action=mv_persisted_create mvName={}", definition.name());
    }

    @Override
    public void onRefreshed(MaterializedViewDefinition definition) {
        Instant refreshedAt = definition.lastRefreshSnapshot().orElse(Instant.now());
        MaterializedViewMetadata metadata = new MaterializedViewMetadata(
                definition.name(),
                definition.query(),
                MaterializedViewState.ACTIVE,
                definition.refreshStrategy().name(),
                definition.createdAt(),
                refreshedAt);
        store.save(metadata);
        LOG.info("action=mv_persisted_refresh mvName={}", definition.name());
    }

    @Override
    public void onDropped(String mvName) {
        store.delete(mvName);
        LOG.info("action=mv_persisted_drop mvName={}", mvName);
    }

    private MaterializedViewMetadata toMetadata(MaterializedViewDefinition def, MaterializedViewState state) {
        return new MaterializedViewMetadata(
                def.name(),
                def.query(),
                state,
                def.refreshStrategy().name(),
                def.createdAt(),
                def.lastRefreshSnapshot().orElse(null));
    }
}
