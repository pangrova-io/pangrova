package org.pangrova.coordinator.file;

import org.pangrova.coordinator.file.FileSpaceAccessService.FileAccessGrant;
import org.pangrova.spi.storage.file.FileListing;

/** Lists one Worker page for an already-issued file access grant. */
@FunctionalInterface
public interface FileListingClient {

    FileListing list(FileAccessGrant grant, String pageToken);
}
