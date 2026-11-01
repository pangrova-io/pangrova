package org.pangrova.spi.connector.data;

import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.spi.connector.data.page.Page;

/**
 * Provides a streaming interface for reading data from a connector split.
 *
 * <p>The engine calls {@link #getNextPage()} repeatedly to read data until
 * {@link #isFinished()} returns {@code true}, then calls {@link #close()}
 * to release resources.</p>
 */
@Stable(since = "1.0")
public interface ConnectorPageSource {

    /**
     * Returns the next page of data, or {@code null} if no data is
     * currently available. Callers should check {@link #isFinished()}
     * to distinguish "temporarily empty" from "permanently done".
     *
     * @return a page of data, or null if none available yet
     */
    default Page getNextPage() {
        return null;
    }

    /**
     * Returns the next native page of data, or {@code null} if not
     * supported or no data is currently available.
     *
     * @return a NativePage wrapping IPC record batch data, or null
     */
    default NativePage getNextNativePage() {
        return null;
    }

    /**
     * Returns the preferred output format for this source.
     * Consumers dispatch to {@link #getNextPage()} or {@link #getNextNativePage()}
     * based on the returned format.
     *
     * @return the preferred page output format (default: ROW)
     */
    default PageOutputFormat preferredFormat() {
        return PageOutputFormat.ROW;
    }

    /**
     * Returns {@code true} if all data has been read from this source.
     *
     * @return true when no more data will be produced
     */
    boolean isFinished();

    /**
     * Releases any resources held by this page source.
     */
    void close();
}
