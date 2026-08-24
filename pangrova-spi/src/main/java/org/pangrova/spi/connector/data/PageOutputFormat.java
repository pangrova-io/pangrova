package org.pangrova.spi.connector.data;

import org.pangrova.spi.annotation.Experimental;

/**
 * Declares the preferred output format for a {@link ConnectorPageSource}.
 * Consumers use this to dispatch to the appropriate read method without
 * if-else chains.
 */
@Experimental
public enum PageOutputFormat {
    /** Row-based {@link org.pangrova.spi.connector.data.page.Page} output. */
    ROW,
    /** Arrow-native {@link org.pangrova.spi.connector.data.page.NativePage} output. */
    NATIVE
}
