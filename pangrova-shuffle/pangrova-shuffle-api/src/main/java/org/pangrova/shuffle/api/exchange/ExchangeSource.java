package org.pangrova.shuffle.api.exchange;

import java.io.Closeable;

import org.pangrova.spi.connector.data.page.NativePage;

/**
 * Read-side endpoint of an exchange channel between query stages.
 *
 * <p>An {@code ExchangeSource} provides columnar Arrow IPC data written by
 * an upstream {@link ExchangeSink}. The downstream stage pulls data by
 * repeatedly calling {@link #getNextNativePage()} until {@link #isFinished()}
 * returns {@code true}.</p>
 *
 * <h3>Lifecycle contract</h3>
 * <ol>
 *   <li>Created via {@link ExchangeChannelManager#createSource}</li>
 *   <li>{@link #getNextNativePage()} — pull columnar data until finished</li>
 *   <li>{@link #isFinished()} — check whether all data has been consumed</li>
 *   <li>{@link #close()} — release all resources</li>
 * </ol>
 *
 * <p>{@link #getNextNativePage()} may return {@code null} when no data is
 * currently available but the source is not yet finished. Callers should
 * retry after a short delay or use a notification mechanism.</p>
 *
 * <p>Implementations must be thread-safe.</p>
 *
 * @see ExchangeChannelManager
 * @see ExchangeSink
 */
public interface ExchangeSource extends Closeable {

    /**
     * Pulls the next available Arrow IPC page from this exchange source.
     *
     * @return the next native page, or {@code null} if no data is currently
     *         available (check {@link #isFinished()} to distinguish between
     *         temporary unavailability and end-of-stream)
     * @throws IllegalStateException if this source has been closed
     */
    NativePage getNextNativePage();

    /**
     * Returns whether all data has been consumed from this source.
     *
     * <p>Once this method returns {@code true}, all subsequent calls to
     * {@link #getNextNativePage()} will return {@code null}.</p>
     *
     * @return {@code true} if the upstream sink has finished and all buffered
     *         data has been consumed
     */
    boolean isFinished();
}
