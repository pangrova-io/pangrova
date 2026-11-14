package org.pangrova.persistence.catalog.api.exception;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;

/** A stale deployment/checkpoint or a reused run ID with different content. No write was accepted. */
public final class MetadataSyncConflictException extends MetastoreException {
    public MetadataSyncConflictException(String message) {
        super(message);
    }
}
