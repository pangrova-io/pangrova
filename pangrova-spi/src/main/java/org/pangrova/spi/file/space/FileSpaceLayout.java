package org.pangrova.spi.file.space;

import java.net.URI;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;

/**
 * Physical key layout for Volumes and Stages inside one storage profile.
 *
 * <p>Managed and internal spaces use their persistent registry root inside
 * the profile's prefix, independent of their current name. External spaces root at the declared
 * location, which must stay inside the profile's prefix. The layout is pure
 * string logic so the same keys are derived on the control and data planes.</p>
 */
@Experimental
public final class FileSpaceLayout {

    private FileSpaceLayout() {
    }

    /** Returns the bucket-relative root for a managed or internal space. */
    public static String managedBaseKey(FileSpace space) {
        if (space.external()) {
            throw new IllegalArgumentException("External spaces do not have a managed storage root");
        }
        return space.managedRoot();
    }

    /**
     * Returns the bucket-relative root for an external space.
     *
     * @throws PangrovaException {@code FILE-0006} when the location escapes the
     *                           profile prefix
     */
    public static String externalBaseKey(FileSpace space, StorageProfile profile) {
        String location = space.location();
        if (profile.type() == StorageType.LOCAL) {
            return localExternalKey(location, profile);
        }
        if (!profile.matchesUrl(location)) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                    "Location '" + location + "' is outside profile '" + profile.name() + "'");
        }
        URI uri = URI.create(location);
        return uri.getRawPath().substring(1);
    }

    private static String localExternalKey(String location, StorageProfile profile) {
        try {
            URI uri = URI.create(location);
            if (!"file".equalsIgnoreCase(uri.getScheme())) {
                throw new IllegalArgumentException("LOCAL external spaces require a file URI");
            }
            var root = java.nio.file.Path.of(profile.bucket()).toAbsolutePath().normalize();
            var scope = root.resolve(profile.prefix() == null ? "" : profile.prefix()).normalize();
            var target = java.nio.file.Path.of(uri).normalize();
            if (!scope.startsWith(root) || !target.startsWith(scope)) {
                throw new IllegalArgumentException("Location is outside the profile prefix");
            }
            return root.relativize(target).toString().replace(java.io.File.separatorChar, '/');
        } catch (IllegalArgumentException failure) {
            throw new PangrovaException(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED,
                    "Invalid LOCAL external location for profile '" + profile.name() + "'", failure);
        }
    }

    /** Returns the full bucket key for a managed space under a profile. */
    public static String managedKey(FileSpace space, StorageProfile profile) {
        return withPrefix(profile, managedBaseKey(space));
    }

    private static String withPrefix(StorageProfile profile, String key) {
        String root = profile.prefix();
        if (root == null || root.isEmpty()) {
            return key;
        }
        String normalized = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        return key.isEmpty() ? normalized : normalized + "/" + key;
    }
}
