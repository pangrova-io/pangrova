package org.pangrova.worker.ai.blob;

import java.io.IOException;
import java.util.List;

/**
 * Scans blob storage directories and retrieves file metadata.
 *
 * <p>Implementations bridge to the underlying storage system (S3, GCS, HDFS, local FS)
 * to list blob files with their modification timestamps. Used by {@link BlobOrphanCleaner}
 * to identify unreferenced blobs eligible for deletion.</p>
 */
public interface BlobStorageScanner {

    /**
     * Lists all blob files under the given directory prefix with their metadata.
     *
     * @param directoryPrefix storage path prefix to scan (e.g., {table_location}/blob/{col}/)
     * @return list of blob file metadata entries; empty if directory does not exist
     * @throws IOException if the storage system cannot be reached or listing fails
     */
    List<BlobFileInfo> listBlobFiles(String directoryPrefix) throws IOException;

    /**
     * Deletes the blob file at the specified URI.
     *
     * @param uri absolute storage URI of the blob file to delete
     * @throws IOException if deletion fails
     */
    void delete(String uri) throws IOException;
}
