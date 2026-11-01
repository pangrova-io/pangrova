package org.pangrova.indexing.control;

import org.pangrova.common.util.DigestUtils;

import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Validated, real external-store finalize response with its exact capability-free RPC identity. */
public record ArtifactExchangeFinalization(
        String exchangeId, String registrationSha256, String shuffleId, long planEpoch,
        ArtifactExecutionOwner finalizingOwner, int producerCount, String producerSetSha256,
        List<Partition> partitions, long totalBytes, long totalRows,
        String responseBase64, String responseSha256) {
    public ArtifactExchangeFinalization {
        ArtifactExchangeRegistration.requireText(exchangeId);
        ArtifactExchangeRegistration.requireText(shuffleId);
        ArtifactExchangeRegistration.requireSha256(registrationSha256);
        ArtifactExchangeRegistration.requireSha256(producerSetSha256);
        ArtifactExchangeRegistration.requireSha256(responseSha256);
        Objects.requireNonNull(finalizingOwner, "finalizingOwner");
        if (planEpoch <= 0 || producerCount <= 0 || totalBytes < 0 || totalRows < 0) {
            throw new IllegalArgumentException("Invalid external finalize identity or totals");
        }
        byte[] response = Base64.getDecoder().decode(responseBase64);
        if (response.length == 0 || response.length > 16 * 1024 * 1024
                || !DigestUtils.matchesSha256(response, responseSha256)
                || !Base64.getEncoder().encodeToString(response).equals(responseBase64)) {
            throw new IllegalArgumentException("External finalize response checksum or size is invalid");
        }
        partitions = partitions.stream().sorted(Comparator.comparingInt(Partition::partitionId)).toList();
        long bytes = 0;
        long rows = 0;
        for (int i = 0; i < partitions.size(); i++) {
            var partition = partitions.get(i);
            if (partition.partitionId() != i) {
                throw new IllegalArgumentException("Finalize partition cohort is not exact");
            }
            bytes = Math.addExact(bytes, partition.totalBytes());
            rows = Math.addExact(rows, partition.totalRows());
        }
        if (partitions.isEmpty() || bytes != totalBytes || rows != totalRows) {
            throw new IllegalArgumentException("Finalize partition totals are incomplete or inconsistent");
        }
    }

    public record Partition(int partitionId, long totalBytes, long totalRows,
            long committedLastSequence, String manifestSha256) {
        public Partition {
            ArtifactExchangeRegistration.requireSha256(manifestSha256);
            if (partitionId < 0 || totalBytes < 0 || totalRows < 0 || committedLastSequence < -1) {
                throw new IllegalArgumentException("Invalid finalized partition");
            }
        }
    }

    /** Validates against the immutable server allocation, not a freshly enumerated source cohort. */
    public void requireRegistration(ArtifactExchangeRegistration registration) {
        if (!exchangeId.equals(registration.exchangeId()) || !registrationSha256.equals(registration.sha256())
                || !shuffleId.equals(registration.shuffleId()) || planEpoch != registration.planEpoch()
                || producerCount != registration.producerCount()
                || !producerSetSha256.equals(registration.producerSetSha256())
                || partitions.size() != registration.partitionCount()) {
            throw new IllegalArgumentException("Finalize evidence differs from its exact registered allocation");
        }
    }

    public byte[] responseBytes() { return Base64.getDecoder().decode(responseBase64); }
    public byte[] canonicalBytes() { return ArtifactExchangeRegistration.encode(this); }
    public String sha256() { return DigestUtils.sha256Hex(canonicalBytes()); }

    public static ArtifactExchangeFinalization decode(byte[] bytes, String sha256) {
        return ArtifactExchangeRegistration.decode(bytes, sha256, ArtifactExchangeFinalization.class);
    }
}
