package org.pangrova.connector.api.index;

import org.pangrova.connector.api.index.CommitEvent;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Functional interface for receiving commit events from connectors.
 *
 * <p>The indexing module registers a {@code CommitListener} with each
 * connector to receive notifications about data file changes, enabling
 * incremental index maintenance.
 */
@Experimental
@FunctionalInterface
public interface CommitListener {

    /**
     * Called when a commit occurs in the connector's table storage.
     *
     * @param event the commit event containing added/removed file information
     */
    void onCommit(CommitEvent event);
}
