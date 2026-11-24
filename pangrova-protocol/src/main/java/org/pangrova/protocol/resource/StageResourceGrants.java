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
        requireAccountingDomain(grant.getJvmHeapBytes(), "jvm_heap_bytes");
        requireAccountingDomain(grant.getNativeMemoryBytes(), "native_memory_bytes");
        requireAccountingDomain(grant.getShuffleSendBytes(), "shuffle_send_bytes");
        requireAccountingDomain(grant.getShuffleReceiveBytes(), "shuffle_receive_bytes");
        requireAccountingDomain(grant.getSpillBytes(), "spill_bytes");
        requireAccountingDomain(grant.getObjectStoreReadBytes(), "object_store_read_bytes");
        requireAccountingDomain(grant.getCandidateRows(), "candidate_rows");
        requireAccountingDomain(grant.getCandidateBytes(), "candidate_bytes");
        requireAccountingDomain(grant.getFillKeys(), "fill_keys");
        requireAccountingDomain(grant.getProviderConcurrency(), "provider_concurrency");
        requireAccountingDomain(grant.getMediaBytes(), "media_bytes");
        requireAccountingDomain(grant.getObjectStoreWriteBytes(), "object_store_write_bytes");
        requireAccountingDomain(grant.getShuffleBufferBytes(), "shuffle_buffer_bytes");
        requireAccountingDomain(grant.getProviderAttempts(), "provider_attempts");
        requireAccountingDomain(grant.getProviderRequestBytes(), "provider_request_bytes");
        requireAccountingDomain(grant.getProviderResponseBytes(), "provider_response_bytes");
        requireAccountingDomain(grant.getProviderItems(), "provider_items");
    }

    private static void requireAccountingDomain(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException("Resource grant exceeds Java accounting domain: " + fieldName);
        }
    }
}
