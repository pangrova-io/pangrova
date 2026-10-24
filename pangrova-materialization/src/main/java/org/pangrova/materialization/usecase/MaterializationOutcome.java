package org.pangrova.materialization.usecase;

import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationObject;

/**
 * Result of a create or refresh command.
 *
 * @param object resulting object header
 * @param generation published generation
 * @param newlyPublished false when an idempotent replay returned the existing result
 */
public record MaterializationOutcome(
        MaterializationObject object,
        Generation generation,
        boolean newlyPublished) {

    public MaterializationOutcome {
        if (object == null || generation == null) {
            throw new IllegalArgumentException("object and generation must not be null");
        }
        if (object.publishedGeneration().isEmpty()
                || !object.publishedGeneration().orElseThrow().equals(generation.id())) {
            throw new IllegalArgumentException("outcome generation must be the published head");
        }
    }
}
