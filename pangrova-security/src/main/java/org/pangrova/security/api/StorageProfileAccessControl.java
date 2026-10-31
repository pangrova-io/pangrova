package org.pangrova.security.api;

import org.pangrova.common.api.annotation.Experimental;

/** Authorization for account-scoped storage profile management. */
@Experimental
public interface StorageProfileAccessControl {

    /** Deny by default when a provider has not implemented profile visibility. */
    default void checkCanViewStorageProfile(UserIdentity user, String profileName) {
        throw new AccessDeniedException("VIEW_STORAGE_PROFILE is required on storage profile '"
                + profileName + "'");
    }

    /** Deny by default so older access-control providers cannot silently permit mutations. */
    default void checkCanManageStorageProfile(UserIdentity user, String profileName) {
        throw new AccessDeniedException("MANAGE_STORAGE_PROFILE is required on storage profile '"
                + profileName + "'");
    }

    /** Deny by default: visibility or management never authorizes reading source bytes. */
    default void checkCanReadStorageProfile(UserIdentity user, String profileName) {
        throw new AccessDeniedException("READ_STORAGE_PROFILE is required on storage profile '"
                + profileName + "'");
    }
}
