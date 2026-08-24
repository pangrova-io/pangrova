package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Triggers incremental index rebuilds when a table write completes.
 *
 * <p>Wired as a {@code Consumer<String>} callback from the DML write path.
 * For each write-completed table, finds all READY indexes and submits incremental
 * build tasks to {@link IndexBuildCoordinator}.</p>
 *
 * <p>Thread-safe: all mutable state is in the injected collaborators.</p>
 */
public final class IndexAutoRebuildTrigger implements Consumer<String> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexAutoRebuildTrigger.class);

    private final IndexMetadataStore metadataStore;
    private final IndexBuildCoordinator buildCoordinator;

    public IndexAutoRebuildTrigger(IndexMetadataStore metadataStore,
                                   IndexBuildCoordinator buildCoordinator) {
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.buildCoordinator = Objects.requireNonNull(buildCoordinator, "buildCoordinator");
    }

    @Override
    public void accept(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            LOG.debug("event=index_auto_rebuild_skip reason=empty_table_name");
            return;
        }
        if (!buildCoordinator.canAcceptBuild()) {
            LOG.info("event=index_auto_rebuild_skip table={} reason=build_pool_saturated",
                    tableName);
            return;
        }
        triggerRebuilds(tableName);
    }

    private void triggerRebuilds(String tableName) {
        String[] parts = parseTableName(tableName);
        if (parts == null) {
            LOG.debug("event=index_auto_rebuild_skip table={} reason=unparseable_name",
                    tableName);
            return;
        }
        List<IndexDescriptor> readyIndexes = findReadyIndexes(parts);
        if (readyIndexes.isEmpty()) {
            LOG.debug("event=index_auto_rebuild_skip table={} reason=no_ready_indexes",
                    tableName);
            return;
        }
        int triggered = submitRebuilds(readyIndexes, tableName);
        LOG.info("event=index_auto_rebuild_complete table={} readyIndexes={} triggered={}",
                tableName, readyIndexes.size(), triggered);
    }

    private List<IndexDescriptor> findReadyIndexes(String[] parts) {
        return metadataStore.findIndexesByTable(parts[0], parts[1], parts[2]).stream()
                .filter(d -> d.state() == IndexState.READY)
                .toList();
    }

    private int submitRebuilds(List<IndexDescriptor> indexes, String tableName) {
        int triggered = 0;
        for (IndexDescriptor descriptor : indexes) {
            try {
                buildCoordinator.submitBuildTask(descriptor, List.of(), null, null);
                triggered++;
                LOG.info("event=index_auto_rebuild_triggered table={} indexId={}",
                        tableName, descriptor.indexId());
            } catch (Exception e) {
                LOG.warn("event=index_auto_rebuild_failed table={} indexId={} error={}",
                        tableName, descriptor.indexId(), e.getMessage());
            }
        }
        return triggered;
    }

    /**
     * Parses a fully qualified table name into [catalog, schema, table].
     */
    static String[] parseTableName(String tableName) {
        String[] parts = tableName.split("\\.", 3);
        return switch (parts.length) {
            case 3 -> parts;
            case 2 -> new String[]{"default", parts[0], parts[1]};
            case 1 -> new String[]{"default", "default", parts[0]};
            default -> null;
        };
    }
}
