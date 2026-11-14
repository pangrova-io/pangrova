package org.pangrova.filesystem.api.blob;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Map;

/** Creates one governed FILE backend for a Worker. Discovery must not allocate backend resources. */
@Experimental
public interface BlobStorageFactory {
    int API_VERSION = 1;

    /** Stable deployment ID selected by the host's storage-backend configuration. */
    String providerId();

    int apiVersion();

    /** Validates the selected backend's settings and creates an instance owned by the caller. */
    BlobStorage create(Map<String, String> configuration);
}
