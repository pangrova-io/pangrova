package org.pangrova.filesystem.api.object;

import java.util.Objects;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.filesystem.api.profile.StorageProfile;

/**
 * Resolved rooting of a Volume or Stage inside one storage profile.
 *
 * <p>{@code baseKey} is the bucket-relative prefix owned by the space. Paths
 * supplied by users are relative to that prefix; {@link #key(String)} is the only
 * supported way to build an object key so traversal and separator tricks are
 * rejected before any storage call.</p>
 */
@Experimental
public record ObjectLocation(StorageProfile profile, String baseKey) {

    public ObjectLocation {
        Objects.requireNonNull(profile, "profile must not be null");
        baseKey = normalizeBase(baseKey, profile);
    }

    /** Resolves a user path to a bucket key, rejecting unsafe segments. */
    public String key(String path) {
        if (path == null) {
            throw invalid("path must not be null");
        }
        String relative = path.startsWith("/") ? path.substring(1) : path;
        if (relative.isEmpty()) {
            return baseKey;
        }
        if (relative.indexOf('%') >= 0 || relative.indexOf('\\') >= 0
                || relative.indexOf('?') >= 0 || relative.indexOf('#') >= 0) {
            throw invalid("path contains a reserved character: " + path);
        }
        for (String segment : relative.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw invalid("path contains an unsafe segment: " + path);
            }
        }
        String key = baseKey.isEmpty() ? relative : baseKey + "/" + relative;
        if (!profile.isKeyWithinPrefix(key)) {
            throw invalid("path escapes the configured storage prefix: " + path);
        }
        return key;
    }

    private static String normalizeBase(String base, StorageProfile profile) {
        String value = base == null ? "" : base.trim();
        while (value.startsWith("/")) {
            value = value.substring(1);
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        if (value.isEmpty()) {
            return "";
        }
        if (!profile.isKeyWithinPrefix(value)) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                    "Storage prefix is outside profile '" + profile.name() + "'");
        }
        return value;
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID, message);
    }
}
