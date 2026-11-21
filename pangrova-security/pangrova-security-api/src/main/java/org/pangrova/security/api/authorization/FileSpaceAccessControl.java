package org.pangrova.security.api.authorization;

import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Authorization for Volume and Stage namespaces.
 *
 * <p>Volumes and stages are separate privilege surfaces even though they share a
 * registry: a volume grant never authorizes a stage operation and vice versa.
 * Every method denies by default so older providers cannot silently permit a
 * new namespace operation.</p>
 */
@Experimental
public interface FileSpaceAccessControl {

    /** Requires CREATE_VOLUME (managed) or CREATE_EXTERNAL_VOLUME (external). */
    default void checkCanCreateVolume(UserIdentity user, String catalog, String schema,
            String name, boolean external) {
        throw deny(external ? "CREATE_EXTERNAL_VOLUME" : "CREATE_VOLUME", "volume", catalog, schema, name);
    }

    /** Requires READ_VOLUME; a null name checks the catalog/schema scope. */
    default void checkCanReadVolume(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("READ_VOLUME", "volume", catalog, schema, name);
    }

    /** Requires WRITE_VOLUME on the exact volume. */
    default void checkCanWriteVolume(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("WRITE_VOLUME", "volume", catalog, schema, name);
    }

    /** Requires MANAGE_VOLUME (ALTER, RENAME, DROP, owner transfer). */
    default void checkCanManageVolume(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("MANAGE_VOLUME", "volume", catalog, schema, name);
    }

    /** Requires CREATE_STAGE on the target schema. */
    default void checkCanCreateStage(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("CREATE_STAGE", "stage", catalog, schema, name);
    }

    /** Requires USAGE_STAGE; a null name checks the catalog/schema scope. */
    default void checkCanUseStage(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("USAGE_STAGE", "stage", catalog, schema, name);
    }

    /** Requires READ_STAGE on the exact stage. */
    default void checkCanReadStage(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("READ_STAGE", "stage", catalog, schema, name);
    }

    /** Requires WRITE_STAGE on the exact stage. */
    default void checkCanWriteStage(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("WRITE_STAGE", "stage", catalog, schema, name);
    }

    /** Requires ownership of the exact stage (ALTER, RENAME, DROP, owner transfer). */
    default void checkCanManageStage(UserIdentity user, String catalog, String schema,
            String name) {
        throw deny("OWNER", "stage", catalog, schema, name);
    }

    private static AccessDeniedException deny(String privilege, String kind, String catalog,
            String schema, String name) {
        String target = name == null ? catalog + "." + schema
                : catalog + "." + schema + "." + name;
        return new AccessDeniedException(
                privilege + " is required on " + kind + " '" + target + "'");
    }
}
