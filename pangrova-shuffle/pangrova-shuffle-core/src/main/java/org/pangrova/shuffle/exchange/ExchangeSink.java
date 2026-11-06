package org.pangrova.shuffle.exchange;

import java.io.Closeable;

import org.pangrova.batch.api.page.NativePage;

/**
 * Write-side endpoint of an exchange channel between query stages.
 *
 * <p>An {@code ExchangeSink} receives columnar Arrow IPC data produced by an
 * upstream stage and buffers it for consumption by a downstream
 * {@link ExchangeSource}.</p>
 *
 * <h3>Lifecycle contract</h3>
 * <ol>
 *   <li>Created via {@link ExchangeChannelManager#createSink}</li>
 *   <li>{@link #addNativePage} — write Arrow IPC pages (zero or more times)</li>
 *   <li>{@link #finish} — signal successful completion, <em>or</em>
 *       {@link #abort} — signal failure and discard buffered data</li>
 *   <li>{@link #close} — release all resources</li>
 * </ol>
 *
 * <p>After {@link #finish} or {@link #abort}, subsequent calls to
 * {@link #addNativePage} must throw {@link IllegalStateException}.</p>
 *
 * <p>Implementations must be thread-safe for concurrent writes from
 * multiple pipeline threads.</p>
 *
 * @see ExchangeChannelManager
 * @see ExchangeSource
 */
public interface ExchangeSink extends Closeable {

    /**
     * Writes a columnar Arrow IPC page to this exchange sink.
     *
     * @param page the native page to write, must not be null
     * @throws NullPointerException  if page is null
     * @throws IllegalStateException if this sink has been finished or aborted
     */
    void addNativePage(NativePage page);

    /**
     * Signals that all data has been successfully written.
     *
     * <p>After this call, buffered data becomes available to the
     * corresponding {@link ExchangeSource}.</p>
     *
     * @throws IllegalStateException if already finished or aborted
     */
    void finish();

    /**
     * Signals upstream failure; buffered data should be discarded.
     */
    void abort();
}
