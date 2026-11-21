package org.pangrova.persistence.workflow.postgresql.projection;

import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.exception.MetadataProjectionScopeException;

/** Reject PostgreSQL identifier limits before accepting a durable request. */
final class ProjectionRequestValidation {
    private static final int MAX_NAME_LENGTH = 255;

    private ProjectionRequestValidation() {}

    static void validate(MetadataProjectionMutation mutation) {
        identifier(mutation.deployment().catalogName());
        identifier(mutation.deployment().incarnationId());
        if (mutation instanceof MetadataProjectionMutation.UpsertTable upsert) {
            identifier(upsert.table().schema());
            identifier(upsert.table().table());
        } else if (mutation instanceof MetadataProjectionMutation.DeleteTable deletion) {
            identifier(deletion.table().schema());
            identifier(deletion.table().table());
        } else if (mutation instanceof MetadataProjectionMutation.ObserveSchemas observation) {
            observation.schemas().forEach(ProjectionRequestValidation::identifier);
        }
    }

    private static void identifier(String value) {
        if (value.indexOf('\0') >= 0
                || value.codePointCount(0, value.length()) > MAX_NAME_LENGTH) {
            throw new MetadataProjectionScopeException(
                    "Projection identifier contains NUL or exceeds storage limits");
        }
    }
}
