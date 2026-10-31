package org.pangrova.databuild.protocol;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.databuild.api.execution.DataBuildExchangeFinalization;
import org.pangrova.databuild.api.execution.DataBuildExchangeRegistration;

/** Canonical external allocation/finalization evidence, without provider access capabilities. */
@Internal
public final class DataBuildExchangeEvidenceCodec {
    private static final String DESCRIPTION = "Durable exchange evidence";
    private static final String INVALID_BYTES = "Durable exchange evidence checksum or size is invalid";

    private DataBuildExchangeEvidenceCodec() {}

    /** Encodes the original external allocation within the persisted byte budget. */
    public static byte[] encode(DataBuildExchangeRegistration registration) {
        return DataBuildCanonicalJson.encode(registration, DESCRIPTION);
    }

    /** Encodes the finalization envelope, including expansion of its exact response bytes. */
    public static byte[] encode(DataBuildExchangeFinalization finalization) {
        return DataBuildCanonicalJson.encode(finalization, DESCRIPTION);
    }

    /** Returns the integrity identity of the immutable server allocation. */
    public static String sha256(DataBuildExchangeRegistration registration) {
        return DigestUtils.sha256Hex(encode(registration));
    }

    /** Returns the integrity identity of the exact finalization envelope. */
    public static String sha256(DataBuildExchangeFinalization finalization) {
        return DigestUtils.sha256Hex(encode(finalization));
    }

    /** Accepts only bounded, correctly hashed, canonical allocation evidence. */
    public static DataBuildExchangeRegistration decodeRegistration(byte[] bytes, String sha256) {
        return DataBuildCanonicalJson.decode(bytes, sha256, DataBuildExchangeRegistration.class,
                DESCRIPTION, INVALID_BYTES);
    }

    /** Accepts only bounded, correctly hashed, canonical finalization evidence. */
    public static DataBuildExchangeFinalization decodeFinalization(byte[] bytes, String sha256) {
        return DataBuildCanonicalJson.decode(bytes, sha256, DataBuildExchangeFinalization.class,
                DESCRIPTION, INVALID_BYTES);
    }

    /** Binds the response to the exact persisted allocation, never a newly enumerated producer cohort. */
    public static void requireRegistration(DataBuildExchangeFinalization finalization,
            DataBuildExchangeRegistration registration) {
        if (!finalization.exchangeId().equals(registration.exchangeId())
                || !finalization.registrationSha256().equals(sha256(registration))
                || !finalization.shuffleId().equals(registration.shuffleId())
                || finalization.planEpoch() != registration.planEpoch()
                || finalization.producerCount() != registration.producerCount()
                || !finalization.producerSetSha256().equals(registration.producerSetSha256())
                || finalization.partitions().size() != registration.partitionCount()) {
            throw new IllegalArgumentException("Finalize evidence differs from its exact registered allocation");
        }
    }
}
