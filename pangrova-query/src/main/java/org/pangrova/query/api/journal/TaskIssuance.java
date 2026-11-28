package org.pangrova.query.api.journal;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Arrays;
import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/** Exact Worker control address, issuing fence and canonical binary admission seal for one attempt. */
@Experimental
public record TaskIssuance(String workerHost, int workerHttpPort, String ownerIncarnation,
        long fencingToken, byte[] admissionSeal) {
    private static final int MAX_SEAL_BYTES = 65_536;
    private static final int MAX_TCP_PORT = 65_535;

    public TaskIssuance {
        requireText(workerHost, "workerHost");
        requireText(ownerIncarnation, "ownerIncarnation");
        Objects.requireNonNull(admissionSeal, "admissionSeal");
        if (workerHttpPort <= 0 || workerHttpPort > MAX_TCP_PORT || fencingToken <= 0
                || admissionSeal.length == 0 || admissionSeal.length > MAX_SEAL_BYTES) {
            throw new IllegalArgumentException("Invalid durable task issuance");
        }
        admissionSeal = admissionSeal.clone();
    }

    @Override public byte[] admissionSeal() { return admissionSeal.clone(); }

    @Override
    public boolean equals(Object other) {
        return other == this || other instanceof TaskIssuance issuance
                && workerHttpPort == issuance.workerHttpPort && fencingToken == issuance.fencingToken
                && workerHost.equals(issuance.workerHost) && ownerIncarnation.equals(issuance.ownerIncarnation)
                && Arrays.equals(admissionSeal, issuance.admissionSeal);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(workerHost, workerHttpPort, ownerIncarnation, fencingToken)
                + Arrays.hashCode(admissionSeal);
    }
}
