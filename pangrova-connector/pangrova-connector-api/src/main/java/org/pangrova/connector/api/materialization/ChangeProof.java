package org.pangrova.connector.api.materialization;

import org.pangrova.connector.api.enums.ChangeSetMode;

import java.time.Instant;
import org.pangrova.common.api.annotation.Stable;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Proof that a change-set plan was fully evaluated and may be applied.
 *
 * <p>An incomplete proof is representable so that connectors can report partial
 * knowledge, but the kernel must reject it before publishing an incremental
 * generation. There is no implicit fallback to a full refresh.
 *
 * @param source source table the proof applies to
 * @param fromToken base snapshot token the proof starts from
 * @param toToken target snapshot token the proof reaches
 * @param mode change-set mode that was proven
 * @param proofFingerprint canonical hash of the proof payload
 * @param strategyVersion version of the connector-side strategy that produced the proof
 * @param verifiedAt instant at which the proof was verified
 * @param complete whether the proof covers the entire change interval
 */
@Stable(since = "1.0")
public record ChangeProof(
        SourceRef source,
        String fromToken,
        String toToken,
        ChangeSetMode mode,
        String proofFingerprint,
        String strategyVersion,
        Instant verifiedAt,
        boolean complete) {

    public ChangeProof {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        fromToken = requireText(fromToken, "fromToken");
        toToken = requireText(toToken, "toToken");
        if (mode == null) {
            throw new IllegalArgumentException("mode must not be null");
        }
        proofFingerprint = requireText(proofFingerprint, "proofFingerprint");
        strategyVersion = requireText(strategyVersion, "strategyVersion");
        if (verifiedAt == null) {
            throw new IllegalArgumentException("verifiedAt must not be null");
        }
    }

    /**
     * Rejects an incomplete proof.
     *
     * @throws IllegalStateException when the proof does not cover the full interval
     */
    public void requireComplete() {
        if (!complete) {
            throw new IllegalStateException(
                    "Incomplete change proof for " + source.canonicalName()
                            + " from " + fromToken + " to " + toToken);
        }
    }
}
