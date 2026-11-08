package org.pangrova.materialization.port;

import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationObject;

/**
 * Result of an idempotent publication.
 *
 * @param object updated object header
 * @param generation published generation
 * @param newlyPublished false when the same command had already published
 */
public record PublicationResult(MaterializationObject object, Generation generation, boolean newlyPublished) {

    public PublicationResult {
        if (object == null || generation == null) {
            throw new IllegalArgumentException("object and generation must not be null");
        }
    }
}
