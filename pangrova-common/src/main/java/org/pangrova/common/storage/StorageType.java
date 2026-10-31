package org.pangrova.common.storage;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Supported external storage backend types for Storage Profiles.
 */
@Experimental
public enum StorageType {
    /** Amazon S3 and S3-compatible services (MinIO, Ceph, etc.). */
    S3,
    /** Alibaba Cloud Object Storage Service (uses S3 protocol internally). */
    OSS,
    /** Google Cloud Storage. */
    GCS,
    /** Azure Data Lake Storage Gen2. */
    ADLS,
    /** Local filesystem (development/testing only). */
    LOCAL
}
