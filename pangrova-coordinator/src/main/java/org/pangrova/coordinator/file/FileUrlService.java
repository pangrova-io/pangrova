package org.pangrova.coordinator.file;

import java.util.Objects;

import org.pangrova.spi.security.UserIdentity;

/**
 * Delivers a time-limited provider URL for one Volume or Stage object.
 *
 * <p>Authorization and signing stay split: this service takes a descriptor from
 * {@link FileSpaceAccessService} and asks the Worker to exchange it for a provider
 * URL. The Coordinator therefore never holds storage credentials and never sees
 * file bytes.</p>
 */
public final class FileUrlService {

    static final long DEFAULT_TTL_SECONDS = 3600;
    static final long MAX_TTL_SECONDS = 7L * 24 * 60 * 60;

    private final FileSpaceAccessService access;
    private final FileUrlClient client;

    public FileUrlService(FileSpaceAccessService access, FileUrlClient client) {
        this.access = Objects.requireNonNull(access, "access must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    /**
     * Issues a presigned URL for one object.
     *
     * @param read       {@code true} for a download URL, {@code false} for an upload URL
     * @param ttlSeconds requested lifetime; non-positive values take the 1-hour default
     */
    public IssuedUrl issue(UserIdentity user, String kind, String catalog, String schema,
            String name, String path, boolean read, long ttlSeconds) {
        long ttl = clamp(ttlSeconds);
        FileSpaceAccessService.FileAccessGrant grant = access.urlGrant(user, kind, catalog,
                schema, name, path, read, ttl);
        FileUrlClient.FileUrl url = client.presign(grant, read, ttl);
        return new IssuedUrl(url.url(), url.expiresAtEpochMillis(), grant.path(),
                read ? "presigned-read" : "presigned-write");
    }

    private static long clamp(long ttlSeconds) {
        if (ttlSeconds <= 0) {
            return DEFAULT_TTL_SECONDS;
        }
        return Math.min(ttlSeconds, MAX_TTL_SECONDS);
    }

    /** Issued URL, its expiry, and the normalized object path it covers. */
    public record IssuedUrl(String url, long expiresAtEpochMillis, String path, String mode) {
    }
}
