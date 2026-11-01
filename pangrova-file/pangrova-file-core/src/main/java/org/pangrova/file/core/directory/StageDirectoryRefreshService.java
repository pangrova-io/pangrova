package org.pangrova.file.core.directory;

import java.util.Objects;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.file.access.FileListingClient;
import org.pangrova.file.core.space.FileSpaceAccessService;
import org.pangrova.persistence.api.file.directory.FileDirectoryIndex;
import org.pangrova.spi.security.FileSpaceAccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.file.space.FileSpace;

/** Refreshes directory metadata through authorized Worker pages, without opening file bytes. */
public final class StageDirectoryRefreshService {
    private static final long MAX_REFRESH_PAGES = 1_000_000;
    private final FileDirectoryIndex index;
    private final FileSpaceAccessService access;
    private final FileListingClient listings;
    private final FileSpaceAccessControl authorization;

    /** Wires the durable index to the authorized Worker listing client. */
    public StageDirectoryRefreshService(FileDirectoryIndex index, FileSpaceAccessService access,
            FileListingClient listings, FileSpaceAccessControl authorization) {
        this.index = Objects.requireNonNull(index, "index");
        this.access = Objects.requireNonNull(access, "access");
        this.listings = Objects.requireNonNull(listings, "listings");
        this.authorization = Objects.requireNonNull(authorization, "authorization");
    }

    /** Returns the number of observations published for the requested subpath. */
    public long refresh(UserIdentity user, FileSpace stage, String subpath) {
        String prefix = FileDirectoryIndex.normalizeSubpath(subpath);
        authorize(user, stage);
        // One immutable storage descriptor for the whole scan. Expiry fails the scan;
        // changing a profile mid-scan must never splice different roots together.
        var grant = access.grant(user, "stages", stage.catalog(), stage.schema(), stage.name(), prefix, "list");
        var token = index.begin(stage, prefix);
        try {
            String checkpoint = null;
            long pages = 0;
            do {
                if (Thread.currentThread().isInterrupted()) {
                    throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE,
                            "Directory refresh was interrupted");
                }
                authorize(user, stage);
                var page = listings.list(grant, checkpoint);
                index.append(token, checkpoint, page);
                checkpoint = page.nextPageToken();
                if (++pages >= MAX_REFRESH_PAGES && checkpoint != null) {
                    throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                            "Directory refresh exceeded one million pages");
                }
            } while (checkpoint != null);
            authorize(user, stage);
            return index.publish(token);
        } catch (RuntimeException failure) {
            try {
                index.abort(token);
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void authorize(UserIdentity user, FileSpace stage) {
        authorization.checkCanManageStage(user, stage.catalog(), stage.schema(), stage.name());
        authorization.checkCanUseStage(user, stage.catalog(), stage.schema(), stage.name());
        authorization.checkCanReadStage(user, stage.catalog(), stage.schema(), stage.name());
    }
}
