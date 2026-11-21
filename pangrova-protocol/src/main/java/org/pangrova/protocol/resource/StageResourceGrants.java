package org.pangrova.protocol.resource;

import java.util.Objects;

import org.pangrova.protocol.v2.StageResourceGrant;

/** Resource-grant invariants shared by transport decoding and execution admission. */
public final class StageResourceGrants {
    private StageResourceGrants() {}

    /** Requires resources for relational execution, including positive native memory. */
    public static void validate(StageResourceGrant grant) {
        validateFields(grant);
        if (grant.getNativeMemoryBytes() <= 0) {
            throw new IllegalArgumentException("Canonical task resource grant is incomplete");
        }
    }

    /** Source preparation requires positive JVM memory and does not require native memory. */
    public static void validateSourcePreparation(StageResourceGrant grant) {
        validateFields(grant);
        if (grant.getJvmHeapBytes() <= 0) {
            throw new IllegalArgumentException("Source preparation requires a JVM memory grant");
        }
    }

    private static void validateFields(StageResourceGrant grant) {
        Objects.requireNonNull(grant, "grant must not be null");
        if (grant.getGrantId().isBlank() || grant.getCpuSlots() <= 0
                || !grant.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Canonical task resource grant is incomplete");
        }
        for (var field : grant.getDescriptorForType().getFields()) {
            Object value = grant.getField(field);
            if ((value instanceof Long longValue && longValue < 0)
                    || (value instanceof Integer integerValue && integerValue < 0)) {
                throw new IllegalArgumentException("Resource grant exceeds Java accounting domain: " + field.getName());
            }
        }
    }
}
