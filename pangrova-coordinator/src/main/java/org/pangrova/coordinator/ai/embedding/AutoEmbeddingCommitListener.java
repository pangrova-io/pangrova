package org.pangrova.coordinator.ai.embedding;

import org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor;
import org.pangrova.planner.embedding.AutoEmbeddingColumnDescriptor.EmbeddingMode;
import org.pangrova.planner.embedding.EmbeddingColumnResolver;
import org.pangrova.spi.connector.index.CommitEvent;
import org.pangrova.spi.connector.index.CommitListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Listens for table commits and enqueues rows for async embedding generation
 * when the committed table has auto-embedding columns.
 *
 * <p>On each commit, resolves auto-embedding column descriptors for the table.
 * For each ASYNC descriptor, enqueues the added files as pending embedding work
 * so the background {@code AsyncEmbeddingDrainer} can scan them for rows
 * needing vector computation.</p>
 */
public final class AutoEmbeddingCommitListener implements CommitListener {

    private static final Logger LOG = LoggerFactory.getLogger(AutoEmbeddingCommitListener.class);

    private final EmbeddingColumnResolver columnResolver;
    private final EmbeddingQueueCoordinator queueCoordinator;
    private final PersistentEmbeddingQueueCoordinator persistentCoordinator;

    /** Creates a listener routing committed rows to the in-memory queue coordinator. */
    public AutoEmbeddingCommitListener(EmbeddingColumnResolver columnResolver,
                                       EmbeddingQueueCoordinator queueCoordinator) {
        if (columnResolver == null) {
            throw new IllegalArgumentException("columnResolver required");
        }
        if (queueCoordinator == null) {
            throw new IllegalArgumentException("queueCoordinator required");
        }
        this.columnResolver = columnResolver;
        this.queueCoordinator = queueCoordinator;
        this.persistentCoordinator = null;
    }

    /**
     * Creates a listener that routes enqueue calls through a persistent coordinator,
     * ensuring work items survive coordinator restarts.
     */
    public AutoEmbeddingCommitListener(EmbeddingColumnResolver columnResolver,
                                       PersistentEmbeddingQueueCoordinator persistentCoordinator) {
        if (columnResolver == null) {
            throw new IllegalArgumentException("columnResolver required");
        }
        if (persistentCoordinator == null) {
            throw new IllegalArgumentException("persistentCoordinator required");
        }
        this.columnResolver = columnResolver;
        this.queueCoordinator = null;
        this.persistentCoordinator = persistentCoordinator;
    }

    @Override
    public void onCommit(CommitEvent event) {
        if (event.addedFiles().isEmpty()) {
            return;
        }

        String fullyQualifiedTable = event.catalogName() + "." + event.schemaName() + "." + event.tableName();
        List<AutoEmbeddingColumnDescriptor> descriptors;
        try {
            descriptors = columnResolver.resolveEmbeddingColumns(fullyQualifiedTable);
        } catch (Exception e) {
            LOG.debug("event=auto_embed_resolve_skip table={} error={}", fullyQualifiedTable, e.getMessage());
            return;
        }

        if (descriptors.isEmpty()) {
            return;
        }

        for (AutoEmbeddingColumnDescriptor desc : descriptors) {
            if (desc.mode() != EmbeddingMode.ASYNC) {
                continue;
            }
            for (String addedFile : event.addedFiles()) {
                try {
                    enqueue(fullyQualifiedTable, addedFile, desc.modelName(), desc.vectorColumn());
                } catch (EmbeddingQueueFullException e) {
                    LOG.warn("event=auto_embed_backpressure table={} file={} model={}",
                            fullyQualifiedTable, addedFile, desc.modelName());
                    return;
                }
            }
            LOG.debug("event=auto_embed_enqueued table={} files={} vectorColumn={} model={}",
                    fullyQualifiedTable, event.addedFiles().size(), desc.vectorColumn(), desc.modelName());
        }
    }

    private void enqueue(String tableName, String rowId, String modelName, String vectorColumn) {
        if (persistentCoordinator != null) {
            persistentCoordinator.enqueue(tableName, rowId, modelName, vectorColumn, "");
        } else {
            queueCoordinator.enqueue(tableName, rowId, modelName, vectorColumn, "");
        }
    }
}
