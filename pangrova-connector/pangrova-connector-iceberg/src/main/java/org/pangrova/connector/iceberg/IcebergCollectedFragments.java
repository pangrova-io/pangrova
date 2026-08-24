package org.pangrova.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

import java.util.List;

/**
 * Holds the aggregated result of collecting files from all fragments.
 * The operation field is null when all fragments were empty/null.
 */
record IcebergCollectedFragments(
        String operation,
        List<DataFile> dataFiles,
        List<DeleteFile> deleteFiles) {}
