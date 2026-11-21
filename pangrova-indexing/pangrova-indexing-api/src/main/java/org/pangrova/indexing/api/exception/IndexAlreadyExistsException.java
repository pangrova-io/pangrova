package org.pangrova.indexing.api.exception;

import org.pangrova.common.exception.PangrovaException;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when attempting to register an index that already exists
 * for the same (catalog, schema, table, indexName) combination.
 */
public class IndexAlreadyExistsException extends PangrovaException {

    public IndexAlreadyExistsException(String indexName, String tableName) {
        super(PangrovaErrorCode.METADATA_INDEX_ALREADY_EXISTS,
                "Index '" + indexName + "' already exists on table '" + tableName + "'");
    }
}
