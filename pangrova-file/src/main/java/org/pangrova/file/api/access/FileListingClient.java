package org.pangrova.file.api.access;

import org.pangrova.file.api.directory.FileListing;

/** Lists one Worker page for an already-issued file access grant. */
@FunctionalInterface
public interface FileListingClient {

    FileListing list(FileAccessGrant grant, String pageToken);
}
