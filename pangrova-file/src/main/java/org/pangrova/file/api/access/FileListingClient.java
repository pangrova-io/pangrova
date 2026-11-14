package org.pangrova.file.api.access;

import org.pangrova.file.api.access.FileAccessGrant;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.filesystem.api.object.FileListing;

/** Lists one Worker page for an already-issued file access grant. */
@FunctionalInterface
@Experimental
public interface FileListingClient {

    FileListing list(FileAccessGrant grant, String pageToken);
}
