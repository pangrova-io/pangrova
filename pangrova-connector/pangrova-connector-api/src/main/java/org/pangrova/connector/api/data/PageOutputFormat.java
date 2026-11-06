package org.pangrova.connector.api.data;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Declares the preferred output format for a {@code ConnectorPageSource}.
 * Consumers use this to dispatch to the appropriate read method without
 * if-else chains.
 */
@Experimental
public enum PageOutputFormat {
    /** Row-based {@link org.pangrova.batch.api.page.Page} output. */
    ROW,
    /** Arrow-native {@link org.pangrova.batch.api.page.NativePage} output. */
    NATIVE
}
