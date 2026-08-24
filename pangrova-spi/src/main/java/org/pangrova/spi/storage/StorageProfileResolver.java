package org.pangrova.spi.storage;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.index.FileSystemAccessor;

/**
 * Resolves a storage URL to the appropriate {@link FileSystemAccessor} using
 * the Storage Profile resolution chain:
 * <ol>
 *   <li>Explicit profile name (SQL: {@code STORAGE 'name'})</li>
 *   <li>Table-level default (property: {@code pangrova.file.storage})</li>
 *   <li>URL auto-matching (bucket + prefix match against registered profiles)</li>
 *   <li>Fallback to catalog's own FileSystemAccessor (same-bucket access)</li>
 * </ol>
 */
@Experimental
public interface StorageProfileResolver {

    /**
     * Resolves the FileSystemAccessor for the given URL using the profile resolution chain.
     *
     * @param url             the storage URL to access (e.g. {@code s3://bucket/key})
     * @param explicitProfile explicitly specified profile name (from SQL STORAGE clause), or null
     * @param tableProfile    table-level default profile name (from table property), or null
     * @return a FileSystemAccessor configured with appropriate credentials
     * @throws IllegalArgumentException if an explicit profile is specified but not found
     */
    FileSystemAccessor resolve(String url, String explicitProfile, String tableProfile);
}
