package org.pangrova.spi.storage.file;

import java.util.List;
import java.util.Objects;

import org.pangrova.spi.annotation.Experimental;

/**
 * One page of a recursive object listing.
 *
 * <p>{@code nextPageToken} is null on the last page. Implementations may return an
 * empty page with a non-null token when a provider scan produced no matching
 * objects within the scanned range; callers must continue until the token is null
 * before treating absence as deletion.</p>
 */
@Experimental
public record FileListing(List<FileStat> files, String nextPageToken) {

    public FileListing {
        files = List.copyOf(Objects.requireNonNull(files, "files must not be null"));
        nextPageToken = nextPageToken == null || nextPageToken.isEmpty() ? null : nextPageToken;
    }

    public boolean hasMore() {
        return nextPageToken != null;
    }
}
