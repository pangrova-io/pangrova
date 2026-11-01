package org.pangrova.spi.connector.index;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Enumerates index types natively supported by data file formats
 * (e.g. Parquet page-level statistics, ORC bloom filters).
 *
 * <p>These are evaluated at the storage layer (L1) before Pangrova's
 * independent indexes (L2), enabling short-circuit skip decisions.
 */
@Experimental
public enum NativeIndexType {

    /** Min/max statistics per row group or page. */
    MIN_MAX,

    /** Bloom filter embedded in the data file format. */
    BLOOM_FILTER,

    /** Bitmap index embedded in the data file format. */
    BITMAP,

    /** Bit-Sliced Index for range queries on numeric columns. */
    BSI
}
