package org.pangrova.persistence.catalog.api.synchronization;

import org.pangrova.persistence.catalog.api.error.MetastoreException;

/** A stale deployment/checkpoint or a reused run ID with different content. No write was accepted. */
public final class MetadataSyncConflictException extends MetastoreException {
    public MetadataSyncConflictException(String message) {
        super(message);
    }
}
