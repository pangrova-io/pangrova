package org.pangrova.persistence.file.api.directory;

import org.pangrova.file.api.directory.DirectoryRefresh;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.file.api.directory.FileDirectoryReference;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.file.api.space.FileSpace;

/** Durable directory metadata with attempt fencing and complete-scan publication; no bytes or database handles. */
@Experimental
public interface FileDirectoryIndex extends DirectoryRefresh {
    /** Resolves the exact captured Stage definition to an immutable directory scan identity. */
    FileDirectoryReference reference(FileSpace space);

    /** Reads a diagnostic page under the exact captured Stage definition; not a relational execution path. */
    List<FileStat> page(FileSpace space, String after, int limit);

}
