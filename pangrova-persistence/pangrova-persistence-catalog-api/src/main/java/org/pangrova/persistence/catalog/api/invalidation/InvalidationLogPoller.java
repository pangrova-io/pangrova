package org.pangrova.persistence.catalog.api.invalidation;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.enums.InvalidationType;

/**
 * Cross-coordinator invalidation-log participant: readers consume committed entries for
 * local metadata invalidation. The owner retains polling lifecycle and resource closure.
 */
@Experimental
public interface InvalidationLogPoller {

    /** Writes one committed invalidation entry for other coordinators to read. */
    void writeInvalidation(String cacheKey, InvalidationType invalidationType);
}
