package org.pangrova.file.core.access;

import java.util.Objects;

import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.file.spi.access.FileUrlClient;

/**
 * Delivers a time-limited provider URL for one Volume or Stage object.
 *
 * <p>Authorization and signing stay split: this service takes a descriptor from
 * {@link FileSpaceAccessService} and asks the Worker to exchange it for a provider
 * URL. The Coordinator therefore never holds storage credentials and never sees
 * file bytes.</p>
 */
public final class FileUrlService {

    /** Lifetime applied when a caller does not request one. */
    public static final long DEFAULT_TTL_SECONDS = 3600;
    /** Upper bound on any issued URL lifetime. */
    public static final long MAX_TTL_SECONDS = 7L * 24 * 60 * 60;

    private final FileSpaceAccessService access;
    private final FileUrlClient client;
    private final java.util.function.LongSupplier clock;

    /** Verifies provider expiry before reporting a successful issuance. */
    public FileUrlService(FileSpaceAccessService access, FileUrlClient client) {
        this(access, client, System::currentTimeMillis);
    }

    /** Overrides the clock used to verify provider expiry. */
    public FileUrlService(FileSpaceAccessService access, FileUrlClient client,
            java.util.function.LongSupplier clock) {
        this.access = Objects.requireNonNull(access, "access must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.clock = Objects.requireNonNull(clock, "clock");
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
        var authorized = access.urlGrant(user, kind, catalog,
                schema, name, path, read, ttl);
        var grant = authorized.grant();
        FileUrlClient.FileUrl url = client.presign(grant, read, ttl);
        if (url.expiresAtEpochMillis() <= clock.getAsLong()
                || url.expiresAtEpochMillis() > grant.expiresAtEpochMillis()) {
            throw new org.pangrova.security.api.error.AccessDeniedException(
                    "Worker URL expiry is outside its authorizing grant");
        }
        access.urlIssued(authorized, read, ttl);
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
        /** Omits bearer credentials from diagnostics. */
        @Override public String toString() {
            return "IssuedUrl[expiresAtEpochMillis=" + expiresAtEpochMillis + ", mode=" + mode + "]";
        }
    }
}
