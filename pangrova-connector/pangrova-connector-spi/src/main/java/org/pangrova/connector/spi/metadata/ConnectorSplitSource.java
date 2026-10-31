package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

/**
 * Provides an iterator-like interface for retrieving splits from a connector.
 *
 * <p>The engine calls {@link #getNextBatch(int)} repeatedly until
 * {@link #isFinished()} returns {@code true}, then calls {@link #close()}
 * to release resources.</p>
 */
@Experimental
public interface ConnectorSplitSource {

    /**
     * Returns the next batch of splits, up to the specified maximum size.
     *
     * @param maxSize the maximum number of splits to return
     * @return a list of splits, possibly empty if no splits are currently available
     */
    List<ConnectorSplit> getNextBatch(int maxSize);

    /**
     * Returns {@code true} if all splits have been enumerated.
     *
     * @return true when no more splits will be produced
     */
    boolean isFinished();

    /**
     * Releases any resources held by this split source.
     */
    void close();
}
