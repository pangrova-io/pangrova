package org.pangrova.security.api.grant;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/** Exact SQL privilege scope. Null trailing coordinates denote the whole namespace, never a delete wildcard. */
@Experimental
public record PrivilegeScope(ObjectType objectType, String catalog, String schema, String object) {
    private static final int MAX_NAME_LENGTH = 255;

    public PrivilegeScope {
        Objects.requireNonNull(objectType, "objectType");
        requireName(catalog);
        if (objectType == ObjectType.CATALOG || objectType == ObjectType.STORAGE_PROFILE) {
            if (schema != null || object != null) throw new IllegalArgumentException("Unexpected scope coordinates");
        } else {
            requireName(schema);
            if (objectType == ObjectType.SCHEMA) {
                if (object != null) throw new IllegalArgumentException("Schema scope has no object");
            } else {
                requireName(object);
            }
        }
    }

    /** SQL privilege targets cannot silently expand a literal '*' into wildcard authority. */
    public static void requireName(String value) {
        if (value == null || value.isBlank() || value.length() > MAX_NAME_LENGTH || value.indexOf(0) >= 0
                || value.equals("*")) {
            throw new IllegalArgumentException("Expected an exact nonempty name of at most 255 characters");
        }
    }

    @Experimental
    public enum ObjectType { CATALOG, SCHEMA, TABLE, VOLUME, STAGE, STORAGE_PROFILE }
}
