package org.pangrova.connector.spi.data;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Declares the preferred output format for a {@link ConnectorPageSource}.
 * Consumers use this to dispatch to the appropriate read method without
 * if-else chains.
 */
@Experimental
public enum PageOutputFormat {
    /** Row-based {@link org.pangrova.connector.spi.data.page.Page} output. */
    ROW,
    /** Arrow-native {@link org.pangrova.connector.spi.data.page.NativePage} output. */
    NATIVE
}
