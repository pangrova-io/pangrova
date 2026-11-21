package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.connector.api.materialization.CommitEvidence;

/**
 * Connector-side reconciliation of an attempt whose outcome is unknown.
 *
 * <p>The recovery must be authoritative: {@link Committed} means the connector
 * holds an exact commit for the reserved candidate, {@link Uncommitted} means
 * the connector proves that no commit exists, and {@link Unknown} is used for
 * every weaker observation. The kernel never treats a timeout or an empty
 * lookup as proof of an uncommitted attempt.
 */
@Experimental
public interface CommitRecovery {

    /**
     * Resolves the exact outcome of one attempt.
     *
     * @param attempt unsettled attempt
     * @return recovery outcome
     */
    Outcome recover(RefreshAttempt attempt);

    /** Exact outcome of an attempt reconciliation. */
    sealed interface Outcome permits Committed, Uncommitted, Unknown {
    }

    /**
     * Connector proved that the candidate committed.
     *
     * @param evidence exact commit evidence
     */
    record Committed(CommitEvidence evidence) implements Outcome {
        public Committed {
            if (evidence == null) {
                throw new IllegalArgumentException("evidence must not be null");
            }
        }
    }

    /**
     * Connector proved that the candidate never committed.
     *
     * @param reason structured proof detail
     */
    record Uncommitted(String reason) implements Outcome {
        public Uncommitted {
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("reason must not be blank");
            }
        }
    }

    /**
     * The outcome cannot be proven; the candidate must stay quarantined.
     *
     * @param reason ambiguity detail
     */
    record Unknown(String reason) implements Outcome {
        public Unknown {
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("reason must not be blank");
            }
        }
    }
}
