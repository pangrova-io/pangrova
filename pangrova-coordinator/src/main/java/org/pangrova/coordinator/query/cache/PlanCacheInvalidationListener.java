package org.pangrova.coordinator.query.cache;

import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventListener;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.planner.cache.PlanCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Subscribes the {@link PlanCache} to {@link MetadataEvent}s and invalidates
 * cached plans whose RelNode tree references a mutated table.
 *
 * <p>Decision matrix:</p>
 * <ul>
 *   <li>{@code TableCreated} — no-op. A newly-created table cannot be referenced
 *       by an already-cached plan (planning would have failed table resolution).</li>
 *   <li>{@code TableDropped} / {@code TableAltered} / {@code SchemaEvolution} —
 *       invalidate by table ({@code catalog.schema.table}). Schema changes can
 *       silently break a cached plan's column types.</li>
 *   <li>{@code CatalogDropped} / {@code CatalogStateChanged} / {@code MetadataSynced} —
 *       invalidate all. The set of affected tables is unbounded or unknown.</li>
 * </ul>
 *
 * <p>This listener is registered on the {@code MetadataEventDispatcher} during
 * coordinator bootstrap. The dispatcher delivers events asynchronously on its
 * own thread, so invalidation runs off the critical query path.</p>
 */
public final class PlanCacheInvalidationListener implements MetadataEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(PlanCacheInvalidationListener.class);

    private final PlanCache planCache;

    public PlanCacheInvalidationListener(PlanCache planCache) {
        this.planCache = Objects.requireNonNull(planCache, "planCache");
    }

    @Override
    public void onEvent(MetadataEvent event) {
        if (event instanceof MetadataEvent.TableCreatedEvent
                || event instanceof MetadataEvent.CatalogCreatedEvent) {
            // No cached plan can reference a brand-new table or catalog.
            return;
        }
        if (event instanceof MetadataEvent.TableDroppedEvent e) {
            invalidateForTable(e.tableRef(), "table_dropped");
        } else if (event instanceof MetadataEvent.TableAlteredEvent e) {
            invalidateForTable(e.tableRef(), "table_altered");
        } else if (event instanceof MetadataEvent.SchemaEvolutionEvent e) {
            invalidateForTable(e.tableRef(), "schema_evolution");
        } else {
            invalidateAllForCatalogEvent(event);
        }
    }

    private void invalidateAllForCatalogEvent(MetadataEvent event) {
        if (event instanceof MetadataEvent.CatalogDroppedEvent e) {
            LOG.info("event=plan_cache_invalidate_all reason=catalog_dropped catalog={}",
                    e.catalogName());
            planCache.invalidateAll();
        } else if (event instanceof MetadataEvent.CatalogStateChangedEvent e) {
            LOG.info("event=plan_cache_invalidate_all reason=catalog_state_changed catalog={} oldState={} newState={}",
                    e.catalogName(), e.oldState(), e.newState());
            planCache.invalidateAll();
        } else if (event instanceof MetadataEvent.MetadataSyncedEvent e) {
            // External sync may have changed any number of tables; conservative invalidation.
            LOG.info("event=plan_cache_invalidate_all reason=metadata_synced catalog={}",
                    e.catalogName());
            planCache.invalidateAll();
        }
    }

    private void invalidateForTable(QualifiedTableRef ref, String reason) {
        String fullName = ref.catalog() + "." + ref.schema() + "." + ref.table();
        LOG.info("event=plan_cache_invalidate_table reason={} table={}", reason, fullName);
        planCache.invalidateByTable(fullName);
    }
}
