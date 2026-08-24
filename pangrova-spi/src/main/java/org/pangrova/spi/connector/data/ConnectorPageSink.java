package org.pangrova.spi.connector.data;

import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.annotation.Stable;
import org.pangrova.spi.connector.data.page.Page;

/**
 * Writes data to a connector output table using two-phase commit.
 *
 * <p>Lifecycle contract (two-phase commit):
 * <ol>
 *   <li>Call {@link #appendPage(Page)} (or native variant) for each batch.</li>
 *   <li>On success: call {@link #finishWithMetadata()} to close writers and produce
 *       commit metadata. This does NOT commit to the catalog.</li>
 *   <li>On failure: call {@link #abort()} to discard any uncommitted data.</li>
 *   <li>Always call {@link #close()} to release resources (idempotent, does NOT commit).</li>
 * </ol>
 *
 * <p>The coordinator collects commit metadata from all workers and performs
 * a single atomic catalog commit via
 * {@link org.pangrova.spi.connector.metadata.ConnectorMetadata#finishDistributedWrite}.
 */
@Stable(since = "1.0")
public interface ConnectorPageSink {

    /**
     * Appends a page of data to this sink.
     *
     * @param page the page of data to write
     */
    void appendPage(Page page);

    /**
     * Appends a native page of data to this sink.
     *
     * @param page the NativePage wrapping IPC record batch data
     * @throws UnsupportedOperationException if native pages are not supported
     */
    default void appendNativePage(NativePage page) {
        throw new UnsupportedOperationException("NativePage not supported");
    }

    /**
     * Closes writers and returns serialized commit metadata fragments.
     *
     * <p>This method does NOT commit to the catalog. It closes all open
     * file writers, collects file metadata (paths, sizes, partition info,
     * column metrics), and returns the serialized fragments. The coordinator
     * aggregates fragments from all workers and performs a single atomic
     * catalog commit.</p>
     *
     * <p>Must be called exactly once on the success path before {@link #close()}.
     * After this call, no more data can be appended.</p>
     *
     * @return serialized commit metadata fragments (never null, may be empty
     *         for sinks that produce no data files)
     */
    byte[] finishWithMetadata();

    /**
     * Discards all uncommitted data written through this sink.
     * Must be called on the failure path before {@link #close()}.
     */
    void abort();

    /**
     * Releases any resources held by this sink. Does NOT commit data.
     * Always call this in a finally block after {@link #finishWithMetadata()}
     * or {@link #abort()}.
     *
     * <p>This method is idempotent.</p>
     */
    void close();
}
