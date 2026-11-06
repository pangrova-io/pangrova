package org.pangrova.batch.api.page;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Discriminator for the wire format of serialized batch data carried by {@link NativePage}.
 *
 * <p>Consumers must check the format before interpreting the byte payload,
 * because the two formats are structurally incompatible.</p>
 */
@Experimental
public enum BatchFormat {

    /**
     * Columnar IPC streaming record batch produced by the native (C++) engine
     * or received from the data exchange transport.
     */
    ARROW_IPC,

    /**
     * Lightweight custom binary format used by Java connector adapters.
     * Layout: {@code [version][rowCount][colCount][colNames][typed values]}.
     */
    CUSTOM_BINARY
}
