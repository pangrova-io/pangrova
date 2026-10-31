package org.pangrova.file.api.access;


/** Deletes one object through a Worker with the grant's signed descriptor. */
@FunctionalInterface
public interface FileDeletionClient {

    /**
     * Deletes {@code grant}'s path.
     *
     * @param expectedVersion provider version or ETag observed before the delete,
     *                        or null to delete without a precondition
     */
    void delete(FileAccessGrant grant, String expectedVersion);
}
