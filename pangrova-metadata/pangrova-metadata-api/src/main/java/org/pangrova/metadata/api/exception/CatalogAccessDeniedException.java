package org.pangrova.metadata.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/**
 * Thrown when a tenant attempts to access a catalog they are not authorized for.
 *
 * <p>G-META-06 fix: tenant-catalog bindings are now enforced. When a catalog has
 * a non-empty {@code allowedTenants} set and the requesting tenant is not in that
 * set, this exception is raised. The message intentionally does not leak whether
 * the catalog exists — it uses the same wording as "not found" to prevent
 * enumeration attacks.</p>
 */
public class CatalogAccessDeniedException extends MetadataException {

    public CatalogAccessDeniedException(String message) {
        super(PangrovaErrorCode.METADATA_CATALOG_ACCESS_DENIED, message);
    }

    public CatalogAccessDeniedException(String message, Throwable cause) {
        super(PangrovaErrorCode.METADATA_CATALOG_ACCESS_DENIED, message, cause);
    }
}
