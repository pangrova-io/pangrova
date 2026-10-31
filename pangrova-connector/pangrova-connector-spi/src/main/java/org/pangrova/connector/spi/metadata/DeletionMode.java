package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Specifies how a connector implements row deletion.
 *
 * <p>The coordinator uses this to determine the correct write path
 * and commit protocol for DELETE operations. Each mode implies
 * different file-level behavior at the storage layer.</p>
 */
@Experimental
public enum DeletionMode {

    /**
     * Rewrites entire data files excluding deleted rows.
     * Used for Iceberg format version 1 tables that do not support
     * row-level deletes. Higher write amplification but fully compatible.
     */
    COPY_ON_WRITE,

    /**
     * Marks deleted rows via positional references in separate delete files.
     * Read path merges base data files with position delete files.
     */
    POSITION_DELETE,

    /**
     * Marks deleted rows via equality predicates in separate delete files.
     * Read path filters rows matching the equality columns.
     */
    EQUALITY_DELETE,

    /**
     * Marks deleted rows via a compact bitmap (Roaring Bitmap) stored
     * in a Puffin file alongside the data file. O(1) per-row lookup.
     */
    DELETION_VECTOR
}
