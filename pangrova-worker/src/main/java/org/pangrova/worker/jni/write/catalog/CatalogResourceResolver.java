package org.pangrova.worker.jni.write.catalog;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.protocol.v2.TaskRequest;
import java.util.List;

/**
 * Resolves a catalog name to the connector resources needed for write operations.
 *
 * <p>This abstraction decouples the write bridge adapter from the metadata
 * module's {@code CatalogRegistry}, allowing the worker module to remain
 * independent of metadata internals per the module dependency rules.</p>
 *
 * <p>The production implementation is a Worker-local connector runtime. It
 * binds the exact catalog revision and Coordinator-allocated output handle
 * before Native task creation; it never calls connector {@code begin*}.</p>
 */
public interface CatalogResourceResolver {

    /** Resolves the exact resources already bound to this task; callers cannot choose a catalog or table. */
    CatalogResources resolve(String taskId);

    /** Validates and binds all connector resources required by a task. */
    void bindTask(String taskId, TaskRequest request);

    /** Releases the task's catalog revision lease and decoded handle. */
    void releaseTask(String taskId);

    /**
     * Connector resources needed to execute write operations against a catalog.
     *
     * @param metadata         connector metadata for table handle resolution
     * @param pageSinkProvider provider for creating page sinks
     * @param session          connector session for the write operation
     */
    record CatalogResources(
            ConnectorMetadata metadata,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            List<String> writeColumnNames) {

        public CatalogResources {
            if (metadata == null) {
                throw new NullPointerException("metadata must not be null");
            }
            if (pageSinkProvider == null) {
                throw new NullPointerException("pageSinkProvider must not be null");
            }
            if (session == null) {
                throw new NullPointerException("session must not be null");
            }
            if (outputHandle == null) {
                throw new NullPointerException("outputHandle must not be null");
            }
            writeColumnNames = List.copyOf(writeColumnNames);
            if (writeColumnNames.isEmpty()
                    || writeColumnNames.stream().anyMatch(name -> name == null || name.isBlank())) {
                throw new IllegalArgumentException("writeColumnNames must be nonempty and nonblank");
            }
        }
    }
}
