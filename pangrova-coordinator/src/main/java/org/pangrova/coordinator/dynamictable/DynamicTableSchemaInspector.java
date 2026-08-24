package org.pangrova.coordinator.dynamictable;

import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.DynamicTableStateStore;
import org.pangrova.metadata.dynamictable.RefreshHistoryEntry;
import org.pangrova.metadata.dynamictable.RefreshHistoryStore;
import org.pangrova.metadata.event.MetadataEvent;
import org.pangrova.metadata.event.MetadataEventListener;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.planner.mv.MaterializedViewDefinition;
import org.pangrova.planner.mv.MaterializedViewRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Handles SHOW and DESCRIBE operations for Dynamic Tables, plus schema
 * evolution event handling that marks affected DTs for refresh.
 */
public final class DynamicTableSchemaInspector implements MetadataEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicTableSchemaInspector.class);
    private static final int RECENT_REFRESH_LIMIT = 5;

    private final DynamicTableStateStore stateStore;
    private final RefreshHistoryStore historyStore;
    private final MaterializedViewRegistry materializedViewRegistry;

    public DynamicTableSchemaInspector(DynamicTableStateStore stateStore,
                                       RefreshHistoryStore historyStore,
                                       MaterializedViewRegistry materializedViewRegistry) {
        this.stateStore = stateStore;
        this.historyStore = historyStore;
        this.materializedViewRegistry = materializedViewRegistry;
    }

    /**
     * List Dynamic Tables for the given tenant.
     */
    public List<DynamicTableInfo> show(String tenantId) {
        return stateStore.listActiveByTenant(tenantId).stream()
                .map(DynamicTableSchemaInspector::toInfo)
                .toList();
    }

    /**
     * Describe a Dynamic Table with recent refresh history.
     */
    public DynamicTableDetail describe(String dtName) {
        DynamicTableState state = stateStore.loadState(dtName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Dynamic Table not found: " + dtName));
        List<RefreshHistoryEntry> recent = historyStore
                .query(dtName, Instant.EPOCH, Instant.now(), RECENT_REFRESH_LIMIT);
        return toDetail(state, recent);
    }

    @Override
    public void onEvent(MetadataEvent event) {
        if (event instanceof MetadataEvent.SchemaEvolutionEvent evolution) {
            markDynamicTablesForSchemaRefresh(evolution);
        }
    }

    private void markDynamicTablesForSchemaRefresh(MetadataEvent.SchemaEvolutionEvent evolution) {
        QualifiedTableRef ref = evolution.tableRef();
        String sourceTable = ref.catalog() + "." + ref.schema() + "." + ref.table();
        try {
            markAffectedDynamicTables(ref, sourceTable);
        } catch (Exception e) {
            LOG.warn("event=schema_evolution_dt_mark_failed source_table={} error={}",
                    sourceTable, e.getMessage());
        }
    }

    private void markAffectedDynamicTables(QualifiedTableRef ref, String sourceTable) {
        List<DynamicTableState> allDts = stateStore.listAll();
        for (DynamicTableState dt : allDts) {
            if (dt.state() != DynamicTableLifecycleState.ACTIVE) {
                continue;
            }
            Optional<MaterializedViewDefinition> mvDef =
                    materializedViewRegistry.getDefinition(dt.dtName());
            if (mvDef.isPresent() && mvDef.get().query().contains(ref.table())) {
                DynamicTableState refreshed = withResetSchema(dt);
                stateStore.saveState(refreshed);
                LOG.info("event=dt_marked_schema_refresh dt={} source_table={}",
                        dt.dtName(), sourceTable);
            }
        }
    }

    static DynamicTableInfo toInfo(DynamicTableState s) {
        return new DynamicTableInfo(
                s.dtName(), s.tenantId(), s.state(),
                s.effectiveRefreshMode(), s.targetLag(),
                s.lastRefreshTime(), s.totalRefreshCount());
    }

    static DynamicTableDetail toDetail(DynamicTableState s,
                                       List<RefreshHistoryEntry> recent) {
        return new DynamicTableDetail(
                s.dtName(), s.tenantId(), s.state(),
                s.effectiveRefreshMode(), s.targetLag(),
                s.checkpoint(), s.checkpointType(),
                s.sourceSchemaVersion(), s.creatorIdentity(),
                s.lastRefreshTime(), s.totalRefreshCount(),
                s.consecutiveFailures(), s.createdAt(), recent);
    }

    static DynamicTableState withResetSchema(DynamicTableState s) {
        return s.toBuilder()
                .checkpoint(null).checkpointType(null)
                .sourceSchemaVersion(1).sourceSchemaSnapshot(null)
                .consecutiveFailures(0).updatedAt(Instant.now()).build();
    }
}
