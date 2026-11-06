package org.pangrova.persistence.file.api.directory;

import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.file.api.directory.FileDirectoryReference;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.error.FileStorageException;

/** Durable directory metadata with attempt fencing and complete-scan publication; no bytes or database handles. */
@Experimental
public interface FileDirectoryIndex {
    /** Resolves the exact captured Stage definition to an immutable directory scan identity. */
    FileDirectoryReference reference(FileSpace space);

    /** Starts a full or directory-scoped attempt and fences an earlier unfinished scan. */
    RefreshToken begin(FileSpace space, String subpath);

    /** Atomically records exactly the expected page and its next checkpoint. */
    void append(RefreshToken token, String expectedPageToken, FileListing page);

    /** Publishes only a terminal scan; deletion stays within the attempt's directory boundary. */
    long publish(RefreshToken token);

    /** Abandons this attempt without modifying a newer attempt or the published directory. */
    void abort(RefreshToken token);

    /** Reads a diagnostic page under the exact captured Stage definition; not a relational execution path. */
    List<FileStat> page(FileSpace space, String after, int limit);

    /** Normalizes a refresh prefix; empty denotes the entire Stage. */
    static String normalizeSubpath(String subpath) {
        String prefix = subpath == null ? "" : subpath;
        if (prefix.endsWith("/")) prefix = prefix.substring(0, prefix.length() - 1);
        if (!prefix.isEmpty()) validatePath(prefix);
        return prefix;
    }

    /** Requires one exact relative observation path, without changing its spelling. */
    static void validatePath(String path) {
        if (path.isEmpty() || path.startsWith("/") || path.indexOf('\\') >= 0
                || path.indexOf('%') >= 0 || path.indexOf('?') >= 0 || path.indexOf('#') >= 0) {
            throw new FileStorageException("Invalid directory path");
        }
        for (String part : path.split("/", -1)) {
            if (part.isEmpty() || part.equals(".") || part.equals("..")) {
                throw new FileStorageException("Invalid directory path segment");
            }
        }
    }

    /** Attempt identity only; callers authorize access and providers fence stale scan progress. */
    @Experimental
    record RefreshToken(UUID spaceId, UUID refreshId) {
        public RefreshToken {
            Objects.requireNonNull(spaceId, "spaceId");
            Objects.requireNonNull(refreshId, "refreshId");
        }
    }
}
