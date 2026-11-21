package org.pangrova.filesystem.api.blob;

import java.util.Map;

import org.pangrova.common.api.annotation.Experimental;

/** A FILE provider with a dedicated, admission-aware backend for managed COPY staging. */
@Experimental
public interface ManagedBlobStorageFactory extends BlobStorageFactory {
    /** Opens a caller-owned backend with bounded I/O and one admission per payload request. */
    ManagedBlobStorage createForCopy(Map<String, String> configuration);
}
