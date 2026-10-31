package org.pangrova.connector.spi.plugin;

import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.spi.data.ConnectorPageSinkProvider;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.ConnectorSplitManager;
import org.pangrova.connector.spi.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.spi.metadata.HealthCheckResult;
import org.pangrova.connector.spi.metadata.IncrementalScanCapability;
import org.pangrova.connector.spi.metadata.IsolationLevel;

import java.util.Optional;

/**
 * The main entry point for a connector plugin.
 *
 * <p>A connector provides access to metadata, split management, data reading,
 * and optionally data writing for an external data source. The engine interacts
 * with the connector through the sub-interfaces returned by this interface.</p>
 *
 * <p>Connectors that do not support write operations may rely on the default
 * {@link #getPageSinkProvider()} implementation which throws
 * {@link UnsupportedOperationException}.</p>
 */
@Stable(since = "1.0")
public interface Connector {

    /**
     * Returns the metadata interface for this connector.
     *
     * @param session the current session context
     * @return the connector metadata, never null
     */
    ConnectorMetadata getMetadata(ConnectorSession session);

    /** Registers a process-lifecycle listener without opening user-scoped connector metadata. */
    default void registerCommitListener(org.pangrova.connector.spi.index.CommitListener listener) {
        throw new UnsupportedOperationException("Connector does not publish commit events");
    }

    /**
     * Returns the split manager for this connector.
     *
     * @return the split manager, never null
     */
    ConnectorSplitManager getSplitManager();

    /**
     * Returns the page source provider for reading data.
     *
     * @return the page source provider, never null
     */
    ConnectorPageSourceProvider getPageSourceProvider();

    /**
     * Returns the page sink provider for writing data.
     *
     * <p>The default implementation throws {@link UnsupportedOperationException}
     * for connectors that do not support write operations.</p>
     *
     * @return the page sink provider
     * @throws UnsupportedOperationException if write is not supported
     */
    default ConnectorPageSinkProvider getPageSinkProvider() {
        throw new UnsupportedOperationException("Write not supported");
    }

    /**
     * Returns the codec used to transport exact Coordinator-allocated output
     * handles to Workers. Writable connectors must override this method.
     */
    default ConnectorOutputHandleCodec getOutputHandleCodec() {
        throw new UnsupportedOperationException("Distributed write handle codec not supported");
    }

    /** Metadata-only adapter for Native physical-file writes, when actually supported. */
    default Optional<org.pangrova.connector.spi.metadata.NativePhysicalWriteProvider> getNativePhysicalWriteProvider() {
        return Optional.empty();
    }

    /**
     * Returns the materialization source contract when this connector can pin
     * exact immutable source snapshots.
     *
     * <p>Connectors that cannot prove snapshot identity must keep the default
     * empty value; the kernel then rejects materializations that read them
     * instead of planning against a movable current snapshot.
     *
     * @return source contract, or empty if the connector cannot serve one
     */
    default Optional<org.pangrova.connector.spi.materialization.SourceProvider> getMaterializationSourceProvider() {
        return Optional.empty();
    }

    /**
     * Begins a new transaction with the specified isolation level.
     *
     * <p>The default implementation returns {@link ConnectorTransactionHandle#empty()}
     * for connectors that do not support transactions.</p>
     *
     * @param level    the requested isolation level
     * @param readOnly whether the transaction is read-only
     * @return a transaction handle representing the new transaction
     */
    default ConnectorTransactionHandle beginTransaction(IsolationLevel level, boolean readOnly) {
        return ConnectorTransactionHandle.empty();
    }

    /**
     * Commits the specified transaction.
     *
     * <p>The default implementation is a no-op.</p>
     *
     * @param transaction the transaction handle to commit
     */
    default void commit(ConnectorTransactionHandle transaction) {
    }

    /**
     * Rolls back the specified transaction.
     *
     * <p>The default implementation is a no-op.</p>
     *
     * @param transaction the transaction handle to roll back
     */
    default void rollback(ConnectorTransactionHandle transaction) {
    }

    /**
     * Shuts down this connector and releases any held resources.
     *
     * <p>The default implementation is a no-op.</p>
     */
    default void shutdown() {
    }

    /**
     * Returns the incremental scan capability for this connector, if supported.
     *
     * <p>Connectors that support incremental or CDC scans return their
     * capability implementation here, enabling the coordinator to dispatch
     * incremental scans through the standard scan path.</p>
     *
     * @return the incremental scan capability, or empty if not supported
     */
    default Optional<IncrementalScanCapability> getIncrementalScanCapability() {
        return Optional.empty();
    }

    /**
     * Performs a health check on this connector.
     *
     * @return the health check result, never null
     */
    default HealthCheckResult healthCheck() {
        return HealthCheckResult.healthy();
    }

}
